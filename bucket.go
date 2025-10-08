package timeq

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"

	"github.com/otiai10/copy"
	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/vlog"
)

const (
	dataLogName = "dat.log"
)

// ReadOp defines what timeq should do with the data that was read.
type ReadOp int

const (
	// ReadOpPeek preserves the read data. It will be available on the next call to Read().
	ReadOpPeek = 0

	// ReadOpPop removes the read data. It will not be available on the next call to Read().
	ReadOpPop = 1
)

type bucketReadOpFn func(items Items) (ReadOp, error)

type bucketIndex struct {
	Log *index.Writer
	Mem *index.Index
}

type bucket struct {
	dir     string
	key     item.Key
	log     *vlog.Log
	opts    Options
	indexes map[ForkName]bucketIndex
}

var (
	ErrNoSuchFork = errors.New("no fork with this name")
)

func (b *bucket) idxForFork(fork ForkName) (bucketIndex, error) {
	idx, ok := b.indexes[fork]
	if !ok {
		return idx, ErrNoSuchFork
	}

	return idx, nil
}

func recoverIndexFromLog(opts *Options, log *vlog.Log, idxPath string) (*index.Index, error) {
	// We try to re-generate the index from the value log if
	// the index is damaged or missing (in case the value log has some entries).
	//
	// Since we have all the keys and offsets there too,
	// we should be able to recover from that. This will not consider already deleted
	// entries of course, as those are marked in the index file, but not in the value log.
	// It's better to replay old values twice then to loose values.

	var memErr error
	mem, memErr := index.FromVlog(log)
	if memErr != nil {
		// not much we can do for that case:
		return nil, fmt.Errorf("index load failed & could not regenerate: %w", memErr)
	}

	if mem.Len() > 0 {
		opts.Logger.Printf("index is empty, but log is not (%s)", idxPath)
	}

	if err := os.Remove(idxPath); err != nil {
		return nil, fmt.Errorf("index failover: could not remove broken index: %w", err)
	}

	// We should write the repaired index after repair, so we don't have to do it
	// again if this gets interrupted. Also this allows us to just push to the index
	// instead of having logic later that writes the not yet written part.
	if err := index.WriteIndex(mem, idxPath); err != nil {
		return nil, fmt.Errorf("index: write during recover did not work")
	}

	if ln := mem.Len(); ln > 0 {
		opts.Logger.Printf("recovered index with %d entries", ln)
	}

	return mem, nil
}

func idxPath(dir string, fork ForkName) string {
	// If there is no fork we use "idx.log" as file name for backwards compatibility.
	var idxName string
	if fork == "" {
		idxName = "idx.log"
	} else {
		idxName = string(fork) + ".idx.log"
	}

	return filepath.Join(dir, idxName)
}

func loadIndex(idxPath string, log *vlog.Log, opts Options) (bucketIndex, error) {
	mem, err := index.Load(idxPath)
	if err != nil || (mem.NEntries() == 0 && !log.IsEmpty()) {
		mem, err = recoverIndexFromLog(&opts, log, idxPath)
		if err != nil {
			return bucketIndex{}, err
		}
	}

	idxLog, err := index.NewWriter(idxPath, opts.SyncMode&SyncIndex > 0)
	if err != nil {
		return bucketIndex{}, fmt.Errorf("index writer: %w", err)
	}

	return bucketIndex{
		Log: idxLog,
		Mem: mem,
	}, nil
}

func openBucket(dir string, forks []ForkName, opts Options) (buck *bucket, outErr error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	// Setting this allows us to handle mmap() errors gracefully.
	// The typical scenario where those errors happen, are full filesystems.
	// This can happen like this:
	//	// * ftruncate() grows a file beyond the available space without error.
	//   Since the new "space" are just zeros that do not take any physical
	//   space this makes sense.
	// * Accessing this mapped memory however will cause the filesystem to actually
	//   try to serve some more pages, which fails as it's full (would also happen on
	//   hardware failure or similar)
	// * This causes a SIGBUS to be send to our process. By default Go crashes the program
	//   and prints a stack trace. Changing this to a recoverable panic allows us to intervene
	//   and continue execution with a proper error return.
	//
	// Other errors like suddenly deleted database files might cause this too.
	// The drawback of this approach is that this might cause issues if the calling processes
	// also sets this option (but with False!). If this turns out to be a problem we have to
	// introduce an option to disable this error handling.
	debug.SetPanicOnFault(true)

	defer recoverMmapError(&outErr)

	logPath := filepath.Join(dir, dataLogName)
	log, err := vlog.Open(logPath, opts.SyncMode&SyncData > 0)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	forks = append(forks, "")
	indexes := make(map[ForkName]bucketIndex, len(forks)-1)

	var entries item.Off
	for _, fork := range forks {
		idxPath := idxPath(dir, fork)
		idx, err := loadIndex(idxPath, log, opts)
		if err != nil {
			return nil, err
		}

		indexes[fork] = idx
		entries += idx.Mem.NEntries()
	}

	key, err := item.KeyFromString(filepath.Base(dir))
	if err != nil {
		return nil, err
	}

	buck = &bucket{
		dir:     dir,
		key:     item.Key(key),
		log:     log,
		indexes: indexes,
		opts:    opts,
	}

	if buck.AllEmpty() && entries > 0 {
		// This means that the buck is empty, but is still occupying space
		// (i.e. it contains values that were popped already). Situations where
		// this might occur are: a Pop() that was interrupted (e.g. a crash), a
		// RemoveFork() that deleted a fork offline, but could not check if
		// it's empty or some other weird crash situation. This is not really
		// an issue but since we occupy space for no real data we should clean up.
		//
		// We do this by brute force: Close the bucket, remove the directory and let it
		// create again by the logic above. This is an edge case, so it doesn't matter if
		// this is perfectly optimized.
		if err := buck.Close(); err != nil {
			return nil, fmt.Errorf("close for reinit: %w", err)
		}

		if err := removeBucketDir(dir, forks); err != nil {
			return nil, fmt.Errorf("remove for reinit: %w", err)
		}

		return openBucket(dir, forks, opts)
	}

	return buck, nil
}

func (b *bucket) Sync(force bool) error {
	err := b.log.Sync(force)
	for _, idx := range b.indexes {
		err = errors.Join(err, idx.Log.Sync(force))
	}

	return err
}

func (b *bucket) Trailers(fn func(fork ForkName, trailer index.Trailer)) {
	for fork, idx := range b.indexes {
		fn(fork, idx.Mem.Trailer())
	}
}

func (b *bucket) Close() error {
	err := b.log.Close()
	for _, idx := range b.indexes {
		err = errors.Join(err, idx.Log.Close())
	}

	return err
}

func recoverMmapError(dstErr *error) {
	// See comment in Open().
	// NOTE: calling recover() is surprisingly quite expensive.
	// Do not call in this loops.
	if recErr := recover(); recErr != nil {
		*dstErr = fmt.Errorf("panic (check: enough space left / file issues): %v - trace:\n%s", recErr, string(debug.Stack()))
	}
}

// Push expects pre-sorted items!
// If `all` is set, all forks receive the new items.
// If `all` is false, then only the fork with `name` gets the new items.
func (b *bucket) Push(items item.Items, all bool, name ForkName) (outErr error) {
	if len(items) == 0 {
		return nil
	}

	defer recoverMmapError(&outErr)

	loc, err := b.log.Push(items)
	if err != nil {
		return fmt.Errorf("push: log: %w", err)
	}

	if all {
		for name, idx := range b.indexes {
			idx.Mem.Set(loc)
			if err := idx.Log.Push(loc, idx.Mem.Trailer()); err != nil {
				return fmt.Errorf("push: index-log: %s: %w", name, err)
			}
		}
	} else {
		// only push to a certain index if requested.
		idx, err := b.idxForFork(name)
		if err != nil {
			return err
		}

		idx.Mem.Set(loc)
		if err := idx.Log.Push(loc, idx.Mem.Trailer()); err != nil {
			return fmt.Errorf("push: index-log: %s: %w", name, err)
		}
	}

	return nil
}

func (b *bucket) logAt(loc item.Location) vlog.Iter {
	continueOnErr := b.opts.ErrorMode != ErrorModeAbort
	return b.log.At(loc, continueOnErr)
}

// addIter adds a new batchIter to `batchIters` and advances the idxIter.
func (b *bucket) addIter(batchIters *vlog.Iters, idxIter *index.Iter) (bool, error) {
	loc := idxIter.Value()
	batchIter := b.logAt(loc)
	if err := vlog.IterReportNonExhaustError(batchIter.Next(b.log)); err != nil {
		// might be empty or I/O error:
		return false, err
	}

	batchIters.Push(batchIter)
	return !idxIter.Next(), nil
}

func (b *bucket) Read(n int, dst *item.Items, fork ForkName, fn bucketReadOpFn) error {
	if n <= 0 {
		// return nothing.
		return nil
	}

	idx, err := b.idxForFork(fork)
	if err != nil {
		return err
	}

	if dst == nil {
		// just for safety:
		v := Items{}
		dst = &v
	}

	iters, items, _, err := b.peek(n, (*dst)[:0], idx.Mem)
	if err != nil {
		return err
	}

	if cap(*dst) < cap(items) {
		// if we appended beyond what we pre-allocated,
		// then use the newly pre-allocated slice.
		*dst = items
	}

	op, err := fn(items)
	if err != nil {
		return err
	}

	switch op {
	case ReadOpPop:
		if iters != nil {
			if err := b.popSync(idx, iters); err != nil {
				return err
			}
		}
	case ReadOpPeek:
		// nothing to do.
	}

	return nil
}

// peek reads from the bucket, but does not mark the elements as deleted yet.
func (b *bucket) peek(n int, dst item.Items, idx *index.Index) (batchIters *vlog.Iters, outItems item.Items, npopped int, outErr error) {
	defer recoverMmapError(&outErr)

	// Fetch the lowest entry of the index:
	idxIter := idx.Iter()
	if !idxIter.Next() {
		// The index is empty. Nothing to pop.
		return nil, dst, 0, nil
	}

	// initialize with first batch iter:
	batchItersSlice := make(vlog.Iters, 0, 1)
	batchIters = &batchItersSlice
	indexExhausted, err := b.addIter(batchIters, &idxIter)
	if err != nil {
		return nil, dst, 0, err
	}

	if len(*batchIters) == 0 {
		// this should not happen normally, but can possibly
		// in case of broken index or WAL.
		return nil, dst, 0, err
	}

	// Choose the lowest item of all iterators here and make sure the next loop
	// iteration will yield the next highest key.
	var numAppends int
	for numAppends < n && !(*batchIters)[0].Exhausted() {
		var currIter = &(*batchIters)[0]
		dst = append(dst, currIter.Item())
		numAppends++

		// advance current batch iter. We will make sure at the
		// end of the loop that the currently first one gets sorted
		// correctly if it turns out to be out-of-order.
		err := currIter.Next(b.log)
		currKey := currIter.Item().Key
		if err := vlog.IterReportNonExhaustError(err); err != nil {
			return nil, dst, 0, err
		}

		// Check the exhausted state as heap.Fix might change the sorting.
		// NOTE: we could do heap.Pop() here to pop the exhausted iters away,
		// but we need the exhausted iters too give them to popSync() later.
		currIsExhausted := currIter.Exhausted()

		// Repair sorting of the heap as we changed the value of the first iter.
		batchIters.Fix(0)

		// index batch entries might be overlapping. We need to check if the
		// next entry in the index needs to be taken into account for the next
		// iteration. For this we compare the next index entry to the
		// supposedly next batch value.
		if !indexExhausted {
			nextLoc := idxIter.Value()
			if currIsExhausted || nextLoc.Key <= currKey {
				indexExhausted, err = b.addIter(batchIters, &idxIter)
				if err != nil {
					return nil, dst, 0, err
				}
			}
		}
	}

	return batchIters, dst, numAppends, nil
}

func (b *bucket) popSync(idx bucketIndex, batchIters *vlog.Iters) error {
	if batchIters == nil || len(*batchIters) == 0 {
		return nil
	}

	// NOTE: In theory we could also use fallocate(FALLOC_FL_ZERO_RANGE) on
	// ext4 to "put holes" into the log file where we read batches from to save
	// some space early. This would make sense only for very big buckets
	// though. We delete the bucket once it was completely exhausted anyways.

	// Now since we've collected all data we need to remember what we consumed.
	for _, batchIter := range *batchIters {
		if !batchIter.Exhausted() {
			currLoc := batchIter.CurrentLocation()

			// some keys were take from it, but not all (or none)
			// we need to adjust the index to keep those reachable.
			idx.Mem.Set(currLoc)

			if err := idx.Log.Push(currLoc, idx.Mem.Trailer()); err != nil {
				return fmt.Errorf("idxlog: append begun: %w", err)
			}
		}

		// Make sure the previous batch index entry gets deleted:
		idx.Mem.Delete(batchIter.FirstKey())
		deadLoc := item.Location{
			Key: batchIter.FirstKey(),
			Len: 0,
			Off: 0,
		}
		if err := idx.Log.Push(deadLoc, idx.Mem.Trailer()); err != nil {
			return fmt.Errorf("idxlog: append begun: %w", err)
		}
	}

	return idx.Log.Sync(false)
}

func (b *bucket) Delete(fork ForkName, from, to item.Key) (ndeleted int, outErr error) {
	defer recoverMmapError(&outErr)

	if b.key > to {
		// this bucket is safe from the clear.
		// NOTE: We can't check `from` here as we don't know
		// how big our bucket is. Could be more efficient if we did...
		return 0, nil
	}

	if to < from {
		return 0, fmt.Errorf("to < from in bucket delete (%d < %d)", to, from)
	}

	idx, err := b.idxForFork(fork)
	if err != nil {
		return 0, err
	}

	lenBefore := idx.Mem.Len()

	var pushErr error
	var deleteEntries []item.Key
	var partialSetEntries []item.Location

	for iter := idx.Mem.Iter(); iter.Next(); {
		loc := iter.Value()
		if loc.Key > to {
			// this index entry may live untouched.
			break
		}

		// `loc` needs to be at least partly deleted.
		// go figure out what part of it.
		leftLoc := loc
		leftLoc.Len = 0
		rightLoc := item.Location{}

		logIter := b.logAt(loc)
		for err := logIter.Next(b.log); err == nil; err = logIter.Next(b.log) {
			item := logIter.Item()
			if item.Key < from {
				// key is not affected by the deletion; keep it.
				leftLoc.Len++
			}

			if item.Key > to && rightLoc.Len == 0 {
				// keys not affected starting here; save position and then
				// go remember that part later on.
				rightLoc = logIter.CurrentLocation()
				break
			}
		}

		locShrinked := int(loc.Len - leftLoc.Len - rightLoc.Len)
		if locShrinked == 0 {
			// location was not actually affected; nothing to do.
			continue
		}

		// always delete the original entry,
		// we need to do that outside of the iteration,
		// as the iter will break otherwise.
		deleteEntries = append(deleteEntries, loc.Key)
		ndeleted += locShrinked

		if leftLoc.Len > 0 {
			partialSetEntries = append(partialSetEntries, leftLoc)
			pushErr = errors.Join(
				pushErr,
				idx.Log.Push(leftLoc, index.Trailer{
					TotalEntries: lenBefore - loc.Len - leftLoc.Len,
				}),
			)
		}

		if rightLoc.Len > 0 {
			partialSetEntries = append(partialSetEntries, rightLoc)
			pushErr = errors.Join(
				pushErr,
				idx.Log.Push(rightLoc, index.Trailer{
					TotalEntries: lenBefore - item.Off(ndeleted),
				}),
			)
		}
	}

	for _, key := range deleteEntries {
		idx.Mem.Delete(key)
		pushErr = errors.Join(pushErr, idx.Log.Push(item.Location{
			Key: key,
			Len: 0,
			Off: 0,
		}, index.Trailer{
			TotalEntries: lenBefore - item.Off(ndeleted),
		}))
	}

	for _, loc := range partialSetEntries {
		idx.Mem.Set(loc)
	}

	return ndeleted, errors.Join(pushErr, idx.Log.Sync(false))
}

func (b *bucket) AllEmpty() bool {
	for _, idx := range b.indexes {
		if idx.Mem.Len() > 0 {
			return false
		}
	}

	return true
}

func (b *bucket) Empty(fork ForkName) bool {
	idx, err := b.idxForFork(fork)
	if err != nil {
		return true
	}

	return idx.Mem.Len() == 0
}

func (b *bucket) Key() item.Key {
	return b.key
}

func (b *bucket) Len(fork ForkName) int {
	idx, err := b.idxForFork(fork)
	if err != nil {
		return 0
	}

	return int(idx.Mem.Len())
}

func (b *bucket) Fork(src, dst ForkName) error {
	srcIdx, err := b.idxForFork(src)
	if err != nil {
		return err
	}

	// If we fork to `dst`, then dst should better not exist.
	if _, err := b.idxForFork(dst); err == nil {
		return nil // fork exists already.
	}

	dstPath := idxPath(b.dir, dst)
	if err := index.WriteIndex(srcIdx.Mem, dstPath); err != nil {
		return err
	}

	dstIdxLog, err := index.NewWriter(dstPath, b.opts.SyncMode&SyncIndex > 0)
	if err != nil {
		return err
	}

	b.indexes[dst] = bucketIndex{
		Log: dstIdxLog,
		Mem: srcIdx.Mem.Copy(),
	}
	return nil
}

func forkOffline(buckDir string, src, dst ForkName) error {
	dstPath := idxPath(buckDir, dst)
	if _, err := os.Stat(dstPath); err == nil {
		// dst already exists.
		return nil
	}

	srcPath := idxPath(buckDir, src)
	opts := copy.Options{Sync: true}
	return copy.Copy(srcPath, dstPath, opts)
}

func (b *bucket) RemoveFork(fork ForkName) error {
	idx, err := b.idxForFork(fork)
	if err != nil {
		return err
	}

	dstPath := idxPath(b.dir, fork)
	delete(b.indexes, fork)
	return errors.Join(
		idx.Log.Close(),
		os.Remove(dstPath),
	)
}

// like RemoveFork() but used when the bucket is not loaded.
func removeForkOffline(buckDir string, fork ForkName) error {
	// Quick path: bucket was not loaded, so we can just throw out
	// the to-be-removed index file:
	return os.Remove(idxPath(buckDir, fork))
}

func (b *bucket) Forks() []ForkName {
	// NOTE: Why + 1? Because some functions like Open() will
	// append the ""-fork as default to the list, so we spare
	// one allocation if we add one extra.
	forks := make([]ForkName, 0, len(b.indexes)+1)
	for fork := range b.indexes {
		if fork == "" {
			continue
		}

		forks = append(forks, fork)
	}

	// since it comes from a map we should be nice and sort it.
	slices.Sort(forks)
	return forks
}

func filterIsNotExist(err error) error {
	if os.IsNotExist(err) {
		return nil
	}

	return err
}

func removeBucketDir(dir string, forks []ForkName) error {
	// We do this here because os.RemoveAll() is a bit more expensive,
	// as it does some extra syscalls and some portability checks that
	// we do not really need. Just delete them explicitly.
	//
	// We also don't care if the files actually existed, as long as they
	// are gone after this function call.

	var err error
	for _, fork := range forks {
		err = errors.Join(
			err,
			filterIsNotExist(os.Remove(idxPath(dir, fork))),
		)
	}

	return errors.Join(
		err,
		filterIsNotExist(os.Remove(filepath.Join(dir, "dat.log"))),
		filterIsNotExist(os.Remove(filepath.Join(dir, "idx.log"))),
		filterIsNotExist(os.Remove(dir)),
	)
}
