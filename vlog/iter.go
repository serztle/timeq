package vlog

import (
	"errors"

	"github.com/sahib/timeq/item"
)

// NOTE: There is quite some performance potential hidden here,
// if we manage to fit Iter in a single cache line:
//
// Possible ideas to get down from 104 to 64:
//
//   - Always pass Item out on Next() as out param. -> -32
//     -> Not possible, because the item might not be consumed directly
//     as we might realize that another iter has more priority.
//   - Do not use exhausted, set len to 0.
//     -> Does not work, as currLen is zero before last call to Next()
//   - continueOnErr can be part of Log. -8 (if exhausted goes away too)
type Iter struct {
	firstKey         item.Key
	currOff, prevOff item.Off
	item             item.Item
	currLen          item.Off
	exhausted        bool       // Merge flags with currLen to a currLenFlags field, 8 bytes.
	continueOnErr    bool
}

var ErrorIterExhausted = errors.New("Iterator is exhausted!")

func (li *Iter) Next(log *Log) error {
	if li.currLen == 0 || li.exhausted {
		li.exhausted = true
		return ErrorIterExhausted
	}

	if len(log.mmap) > 0 && li.currOff >= item.Off(log.size) {
		// stop iterating when end of log reached.
		li.exhausted = true
		return ErrorIterExhausted
	}

	for {
		if err := log.readItemAt(li.currOff, &li.item); err != nil {
			if !li.continueOnErr {
				li.exhausted = true
				return err
			}

			li.currOff = log.findNextItem(li.currOff)
			if li.currOff >= item.Off(log.size) {
				li.exhausted = true
				return ErrorIterExhausted
			}

			continue
		}

		break
	}

	li.prevOff = li.currOff

	// advance iter to next position:
	li.currOff += item.Off(li.item.StorageSize())
	li.currLen--

	return nil
}

func (li *Iter) Exhausted() bool {
	return li.exhausted
}

// Key returns the key this iterator was created with
// This is not the current key of the item!
func (li *Iter) FirstKey() item.Key {
	return li.firstKey
}

// Item returns the current item.
// It is not valid before Next() has been called.
func (li *Iter) Item() item.Item {
	return li.item
}

// CurrentLocation returns the location of the current entry.
// It is not valid before Next() has been called.
func (li *Iter) CurrentLocation() item.Location {
	return item.Location{
		Key: li.item.Key,
		Off: li.prevOff,
		Len: li.currLen + 1,
	}
}

func IterReportNonExhaustError(err error) error {
	if err == ErrorIterExhausted {
		return nil
	} else {
		return err
	}
}
