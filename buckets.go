package timeq

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/tidwall/btree"
)

// trailerKey is the key to access a index.Trailer for a certain
// Buckets that are not loaded have some info that is easily accessible without
// loading them fully (i.e. the len). Since the Len can be different for each
// fork we need to keep it for each one separately.
type trailerKey struct {
	Key  item.Key
	fork ForkName
}

// small wrapper around buckets that calls Push() without locking.
type tx struct {
	bs *buckets
}

func (tx *tx) Push(items item.Items) error {
	return tx.bs.Push(items, false)
}

type buckets struct {
	mu       sync.Mutex
	dir      string
	tree     btree.Map[item.Key, *bucket]
	trailers map[trailerKey]index.Trailer
	opts     Options
	forks    []ForkName
	readBuf  Items
}

func loadAllBuckets(dir string, opts Options) (*buckets, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read-dir: %w", err)
	}

	var dirsHandled int
	tree := btree.Map[item.Key, *bucket]{}
	trailers := make(map[trailerKey]index.Trailer, len(ents))
	for _, ent := range ents {
		if !ent.IsDir() {
			continue
		}

		buckPath := filepath.Join(dir, ent.Name())
		key, err := item.KeyFromString(filepath.Base(buckPath))
		if err != nil {
			if opts.ErrorMode == ErrorModeAbort {
				return nil, err
			}

			opts.Logger.Printf("failed to parse %s as bucket path\n", buckPath)
			continue
		}

		dirsHandled++

		if err := index.ReadTrailers(buckPath, func(fork string, trailer index.Trailer) {
			// nil entries indicate buckets that were not loaded yet:
			trailers[trailerKey{
				Key:  key,
				fork: ForkName(fork),
			}] = trailer
		}); err != nil {
			// reading trailers is not too fatal, but applications may break in unexpected
			// ways when Len() returns wrong results.
			return nil, err
		}

		tree.Set(key, nil)
	}

	if dirsHandled == 0 && len(ents) > 0 {
		return nil, fmt.Errorf("%s is not empty; refusing to create db", dir)
	}

	bs := &buckets{
		dir:      dir,
		tree:     tree,
		opts:     opts,
		trailers: trailers,
		readBuf:  make(Items, 2000),
	}

	forks, err := bs.fetchForks()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch forks: %w", err)
	}

	bs.forks = forks
	return bs, nil
}

// ValidateBucketKeys checks if the keys in the buckets correspond to the result
// of the key func. Failure here indicates that the key function changed. No error
// does not guarantee that the key func did not change though (e.g. the identity func
// would produce no error in this check)
func (bs *buckets) ValidateBucketKeys(bucketFn func(item.Key) item.Key) error {
	for iter := bs.tree.Iter(); iter.Next(); {
		ik := iter.Key()
		bk := bucketFn(ik)

		if ik != bk {
			return fmt.Errorf(
				"bucket with key %s does not match key func (%d) - did it change",
				ik,
				bk,
			)
		}
	}

	return nil
}

func (bs *buckets) buckPath(key item.Key) string {
	return filepath.Join(bs.dir, key.String())
}

// forKey returns a bucket for the specified key and creates if not there yet.
// `key` must be the lowest key that is stored in this  You cannot just
// use a key that is somewhere in the
func (bs *buckets) forKey(key item.Key) (*bucket, error) {
	buck, _ := bs.tree.Get(key)
	if buck != nil {
		// fast path:
		return buck, nil
	}

	// make room for one so we don't jump over the maximum:
	if err := bs.closeUnused(bs.opts.MaxParallelOpenBuckets - 1); err != nil {
		return nil, err
	}

	var err error
	buck, err = openBucket(bs.buckPath(key), bs.forks, bs.opts)
	if err != nil {
		return nil, err
	}

	bs.tree.Set(key, buck)
	return buck, nil
}

func (bs *buckets) delete(key item.Key) error {
	buck, ok := bs.tree.Get(key)
	if !ok {
		return fmt.Errorf("no bucket with key %v", key)
	}

	for tk := range bs.trailers {
		if tk.Key == key {
			delete(bs.trailers, tk)
		}
	}

	var err error
	var dir string
	if buck != nil {
		// make sure to close the bucket, otherwise we will accumulate mmaps, which
		// will sooner or later lead to memory allocation issues/errors.
		err = buck.Close()
		dir = buck.dir // save on allocation of buckPath()
	} else {
		dir = bs.buckPath(key)
	}

	bs.tree.Delete(key)

	return errors.Join(err, removeBucketDir(dir, bs.forks))
}

type iterMode int

const (
	// includeNil goes over all buckets, including those that are nil (not loaded.)
	includeNil = iterMode(iota)

	// loadedOnly iterates over all buckets that were loaded already.
	loadedOnly

	// load loads all buckets, including those that were not loaded yet.
	load
)

// errIterStop can be returned in Iter's func when you want to stop
// It does not count as error.
var errIterStop = errors.New("iteration stopped")

// Iter iterates over all buckets, starting with the lowest. The buckets include
// unloaded depending on `mode`. The error you return in `fn` will be returned
// by Iter() and iteration immediately stops. If you return errIterStop then
// Iter() will return nil and will also stop the iteration. Note that Iter() honors the
// MaxParallelOpenBuckets option, i.e. when the mode is `Load` it will immediately close
// old buckets again before proceeding.
func (bs *buckets) iter(mode iterMode, fn func(key item.Key, b *bucket) error) error {
	// NOTE: We cannot directly iterate over the tree here, we need to make a copy
	// of they keys, as the btree library does not like if the tree is modified during iteration.
	// Modifications can happen in forKey() (which might close unused buckets) or in the user-supplied
	// function (notably Pop(), which deletes exhausted buckets). By copying it we make sure to
	// iterate over one consistent snapshot. This might need to change if we'd create new buckets
	// in fn() - let's hope that this does not happen.
	keys := bs.tree.Keys()
	for _, key := range keys {
		// Fetch from non-copied tree as this is the one that is modified.
		buck, ok := bs.tree.Get(key)
		if !ok {
			// it was deleted already? Skip it.
			continue
		}

		if buck == nil {
			if mode == loadedOnly {
				continue
			}

			if mode == load {
				// load the bucket fresh from disk.
				// NOTE: This might unload other buckets!
				var err error
				buck, err = bs.forKey(key)
				if err != nil {
					return err
				}
			}
		}

		if err := fn(key, buck); err != nil {
			if err == errIterStop {
				err = nil
			}

			return err
		}
	}

	return nil
}

func (bs *buckets) Sync() error {
	var err error
	bs.mu.Lock()
	defer bs.mu.Unlock()

	_ = bs.iter(loadedOnly, func(_ item.Key, b *bucket) error {
		// try to sync as much as possible:
		err = errors.Join(err, b.Sync(true))
		return nil
	})

	return err
}

func (bs *buckets) Clear() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	return bs.clear()
}

func (bs *buckets) clear() error {
	keys := bs.tree.Keys()
	for _, key := range keys {
		if err := bs.delete(key); err != nil {
			return err
		}
	}

	return nil
}

func (bs *buckets) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	return bs.iter(loadedOnly, func(_ item.Key, b *bucket) error {
		return b.Close()
	})
}

func (bs *buckets) Len(fork ForkName) int {
	var len int
	bs.mu.Lock()
	defer bs.mu.Unlock()

	_ = bs.iter(includeNil, func(key item.Key, b *bucket) error {
		if b == nil {
			trailer, ok := bs.trailers[trailerKey{
				Key:  key,
				fork: fork,
			}]

			if !ok {
				bs.opts.Logger.Printf("bug: no trailer for %v", key)
				return nil
			}

			len += int(trailer.TotalEntries)
			return nil
		}

		len += b.Len(fork)
		return nil
	})

	return len
}

func (bs *buckets) Shovel(dstBs *buckets, fork ForkName) (int, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	dstBs.mu.Lock()
	defer dstBs.mu.Unlock()

	var ntotalcopied int
	err := bs.iter(includeNil, func(key item.Key, _ *bucket) error {
		if _, ok := dstBs.tree.Get(key); !ok {
			// fast path: We can just move the bucket directory.
			dstPath := dstBs.buckPath(key)
			srcPath := bs.buckPath(key)
			dstBs.tree.Set(key, nil)

			if err := index.ReadTrailers(srcPath, func(srcfork string, trailer index.Trailer) {
				if fork == ForkName(srcfork) {
					ntotalcopied += int(trailer.TotalEntries)
				}

				dstBs.trailers[trailerKey{
					Key:  key,
					fork: ForkName(srcfork),
				}] = trailer
			}); err != nil {
				return err
			}

			return moveFileOrDir(srcPath, dstPath)
		}

		// In this case we have to copy the items more intelligently,
		// since we have to append it to the destination

		srcBuck, err := bs.forKey(key)
		if err != nil {
			return err
		}

		// NOTE: This assumes that the destination has the same bucket func.
		dstBuck, err := dstBs.forKey(key)
		if err != nil {
			return err
		}

		return srcBuck.Read(math.MaxInt, &bs.readBuf, fork, func(items item.Items) (ReadOp, error) {
			if err := dstBuck.Push(items, true, fork); err != nil {
				return ReadOpPeek, err
			}

			ntotalcopied += len(items)
			return ReadOpPop, nil
		})
	})

	if err != nil {
		return ntotalcopied, err
	}

	if err := bs.clear(); err != nil {
		return ntotalcopied, err
	}

	return ntotalcopied, err
}

func (bs *buckets) nloaded() int {
	var nloaded int
	bs.tree.Scan(func(_ item.Key, buck *bucket) bool {
		if buck != nil {
			nloaded++
		}
		return true
	})

	return nloaded
}

// closeUnused closes as many buckets as needed to reach `maxBucks` total loaded buckets.
// The closed buckets are marked as nil and can be loaded again afterwards.
// If `maxBucks` is negative, this is a no-op.
func (bs *buckets) closeUnused(maxBucks int) error {
	if maxBucks < 0 {
		// This disables this feature. You likely do not want that.
		return nil
	}

	// Fetch the number of loaded buckets.
	// We could optimize that by having another count for that,
	// but it should be cheap enough and this way we have only one
	// source of truth.
	nloaded := bs.nloaded()
	if nloaded <= maxBucks {
		// nothing to do, this should be the normal case.
		return nil
	}

	var closeErrs error

	// This logic here produces a sequence like this:
	// pivot=4: 4+0, 4-1, 4+1, 4-2, 4+2, 4-3, 4+3, 4-4, 4+4, ...
	//
	// In other words, it alternates around the middle of the buckets and
	// closes buckets that are more in the middle of the queue. This should be
	// a reasonable heuristic for a typical queue system where you pop from end
	// and push to the other one, but seldomly access buckets in the middle
	// range If you're priorities are very random, this will be rather random
	// too though.
	nClosed, nClosable := 0, nloaded-maxBucks
	pivotIdx := bs.tree.Len() / 2
	for idx := 0; idx < bs.tree.Len() && nClosed < nClosable; idx++ {
		realIdx := pivotIdx - idx/2 - 1
		if idx%2 == 0 {
			realIdx = pivotIdx + idx/2
		}

		key, buck, ok := bs.tree.GetAt(realIdx)
		if !ok {
			// should not happen, but better be safe.
			continue
		}

		if buck == nil {
			// already closed.
			continue
		}

		// We need to store the trailers of each fork, so we know how to
		// calculcate the length of the queue without having to load everything.
		buck.Trailers(func(fork ForkName, trailer index.Trailer) {
			bs.trailers[trailerKey{
				Key:  key,
				fork: fork,
			}] = trailer
		})

		if err := buck.Close(); err != nil {
			switch bs.opts.ErrorMode {
			case ErrorModeAbort:
				closeErrs = errors.Join(closeErrs, err)
			case ErrorModeContinue:
				bs.opts.Logger.Printf("failed to reap bucket %s", key)
			}
		}

		bs.tree.Set(key, nil)
		// bs.trailers[key] = trailer
		nClosed++
	}

	return closeErrs
}

// binsplit returns the first index of `items` that would
// not go to the bucket `comp`. There are two assumptions:
//
// * "items" is not empty.
// * "comp" exists for at least one fn(item.Key)
// * The first key in `items` must be fn(key) == comp
//
// If assumptions are not fulfilled you will get bogus results.
func binsplit(items item.Items, comp item.Key, fn func(item.Key) item.Key) int {
	l := len(items)
	if l == 0 {
		return 0
	}
	if l == 1 {
		return 1
	}

	pivotIdx := l / 2
	pivotKey := fn(items[pivotIdx].Key)
	if pivotKey != comp {
		// search left:
		return binsplit(items[:pivotIdx], comp, fn)
	}

	// search right:
	return pivotIdx + binsplit(items[pivotIdx:], comp, fn)
}

func (bs *buckets) Push(items item.Items, locked bool) error {
	if len(items) == 0 {
		return nil
	}

	slices.SortFunc(items, func(i, j item.Item) int {
		return int(i.Key - j.Key)
	})

	if locked {
		bs.mu.Lock()
		defer bs.mu.Unlock()
	}

	return bs.pushSorted(items)
}

// Sort items into the respective buckets:
func (bs *buckets) pushSorted(items item.Items) error {
	for len(items) > 0 {
		keyMod := bs.opts.BucketFunc(items[0].Key)
		nextIdx := binsplit(items, keyMod, bs.opts.BucketFunc)
		buck, err := bs.forKey(keyMod)
		if err != nil {
			if bs.opts.ErrorMode == ErrorModeAbort {
				return fmt.Errorf("bucket: for-key: %w", err)
			}

			bs.opts.Logger.Printf("failed to push: %v", err)
		} else {
			if err := buck.Push(items[:nextIdx], true, ""); err != nil {
				if bs.opts.ErrorMode == ErrorModeAbort {
					return fmt.Errorf("bucket: push: %w", err)
				}

				bs.opts.Logger.Printf("failed to push: %v", err)
			}
		}

		items = items[nextIdx:]
	}

	return nil
}

func (bs *buckets) Read(n int, fork ForkName, fn TransactionFn) error {
	if n < 0 {
		// use max value to select all.
		n = int(^uint(0) >> 1)
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()

	var count = n
	return bs.iter(load, func(key item.Key, b *bucket) error {
		lenBefore := b.Len(fork)

		// wrap the bucket call into something that knows about
		// transactions - bucket itself does not care about that.
		wrappedFn := func(items Items) (ReadOp, error) {
			return fn(&tx{bs}, items)
		}

		if err := b.Read(count, &bs.readBuf, fork, wrappedFn); err != nil {
			if bs.opts.ErrorMode == ErrorModeAbort {
				return err
			}

			// try with the next bucket in the hope that it works:
			bs.opts.Logger.Printf("failed to pop: %v", err)
			return nil
		}

		if b.AllEmpty() {
			if err := bs.delete(key); err != nil {
				return fmt.Errorf("failed to delete bucket: %w", err)
			}
		}

		lenAfter := b.Len(fork)

		count -= (lenBefore - lenAfter)
		if count <= 0 {
			return errIterStop
		}

		return nil
	})
}

func (bs *buckets) Delete(fork ForkName, from, to item.Key) (int, error) {
	var numDeleted int
	var deletableBucks []item.Key

	if to < from {
		return 0, fmt.Errorf("delete: `to` must be >= `from`")
	}

	bs.mu.Lock()
	defer bs.mu.Unlock()

	// use the bucket func to figure out which buckets the range limits would be in.
	// those buckets might not really exist though.
	toBuckKey := bs.opts.BucketFunc(to)
	fromBuckKey := bs.opts.BucketFunc(from)

	iter := bs.tree.Iter()
	if !iter.Seek(fromBuckKey) {
		// all buckets that we have are > to. Nothing to delete.
		return 0, nil
	}

	// Seek() already sets the iter.Value(), so iteration becomes a bit awkward.
	for {
		buckKey := iter.Key()
		if buckKey > toBuckKey {
			// too far, stop it.
			break
		}

		buck, err := bs.forKey(buckKey)
		if err != nil {
			if bs.opts.ErrorMode == ErrorModeAbort {
				return numDeleted, err
			}

			// try with the next bucket in the hope that it works:
			bs.opts.Logger.Printf(
				"failed to open %v for deletion delete : %v",
				buckKey,
				err,
			)
		} else {
			numDeletedOfBucket, err := buck.Delete(fork, from, to)
			if err != nil {
				if bs.opts.ErrorMode == ErrorModeAbort {
					return numDeleted, err
				}

				// try with the next bucket in the hope that it works:
				bs.opts.Logger.Printf("failed to delete : %v", err)
			} else {
				numDeleted += numDeletedOfBucket
				if buck.AllEmpty() {
					deletableBucks = append(deletableBucks, buckKey)
				}
			}
		}

		if !iter.Next() {
			break
		}
	}

	for _, bucketKey := range deletableBucks {
		if err := bs.delete(bucketKey); err != nil {
			return numDeleted, fmt.Errorf("bucket delete: %w", err)
		}
	}

	return numDeleted, nil
}

func (bs *buckets) Fork(src, dst ForkName) error {
	if err := dst.Validate(); err != nil {
		return err
	}

	if slices.Contains(bs.forks, dst) {
		// if no bucket is currently loaded, we still need to check for dupes.
		return nil
	}

	err := bs.iter(includeNil, func(key item.Key, buck *bucket) error {
		if buck != nil {
			return buck.Fork(src, dst)
		}

		buckDir := filepath.Join(bs.dir, key.String())
		return forkOffline(buckDir, src, dst)
	})

	if err != nil {
		return err
	}

	bs.forks = append(bs.forks, dst)
	return nil
}

func (bs *buckets) RemoveFork(fork ForkName) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if err := fork.Validate(); err != nil {
		return err
	}

	// Remove fork from fork list to avoid creating it again:
	bs.forks = slices.DeleteFunc(bs.forks, func(candidate ForkName) bool {
		return fork == candidate
	})

	return bs.iter(includeNil, func(key item.Key, buck *bucket) error {
		if buck != nil {
			if err := buck.RemoveFork(fork); err != nil {
				return err
			}

			// might be empty after deletion, so we can get rid of the
			if !buck.AllEmpty() {
				return nil
			}

			return bs.delete(key)
		}

		// NOTE: In contrast to the "loaded bucket" case above we do not check if the bucket is
		// considered AllEmpty() after the fork deletion. We defer that to the next Open() of this
		// bucket to avoid having to load all buckets here. We can have a clean up  logic in Open()
		// that re-initializes the bucket freshly when the index Len() is zero (and no recover needed).
		buckDir := filepath.Join(bs.dir, key.String())
		return removeForkOffline(buckDir, fork)
	})
}

func (bs *buckets) Forks() []ForkName {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	return bs.forks
}

// fetchForks actually checks the disk to find the current forks.
func (bs *buckets) fetchForks() ([]ForkName, error) {
	var buck *bucket
	for iter := bs.tree.Iter(); iter.Next(); {
		buck = iter.Value()
		if buck == nil {
			continue
		}
	}

	if buck == nil {
		// if no bucket was loaded yet, above for will not find any.
		// no luck, we gonna need to load one for this operation.
		iter := bs.tree.Iter()
		if iter.First() {
			var err error
			buck, err = bs.forKey(iter.Key())
			if err != nil {
				return nil, err
			}
		}
	}

	if buck == nil {
		// still nil? The tree is probably empty.
		return []ForkName{}, nil
	}

	return buck.Forks(), nil
}
