package timeq

import (
	"fmt"
	"os"
	"reflect"
)

// TODO: Examples move.

func ExampleQueue() {
	// Error handling stripped for brevity:
	dir, _ := os.MkdirTemp("", "timeq-example")
	defer os.RemoveAll(dir)

	// Open the queue. If it does not exist, it gets created:
	queue, _ := Open(dir, DefaultOptions())

	// Push some items to it:
	pushItems := make(Items, 0, 10)
	for idx := 0; idx < 10; idx++ {
		pushItems = append(pushItems, Item{
			Key:  Key(idx),
			Blob: []byte(fmt.Sprintf("key_%d", idx)),
		})
	}

	_ = queue.Push(pushItems)

	// Retrieve the same items again:
	_ = queue.Read(10, func(popItems Items) (ReadOp, error) {
		// Just for example purposes, check if they match:
		if reflect.DeepEqual(pushItems, popItems) {
			fmt.Println("They match! :)")
		} else {
			fmt.Println("They do not match! :(")
		}

		return ReadOpPop, nil
	})

	// Output: They match! :)
}

func ExampleQueueFork() {
	// Error handling stripped for brevity:
	dir, _ := os.MkdirTemp("", "timeq-example")
	defer os.RemoveAll(dir)

	// Open the queue. If it does not exist, it gets created:
	queue, _ := Open(dir, DefaultOptions())

	// For the consuming end in half:
	fork, _ := queue.Fork("fork")

	// Push some items to it - they are added to both the regular queue
	// as well to the fork we just created.
	_ = queue.Push(Items{
		Item{
			Key:  123,
			Blob: []byte("some data"),
		},
	})

	// Check the main queue contents:
	_ = queue.Read(1, func(items Items) (ReadOp, error) {
		fmt.Println(string(items[0].Blob))
		return ReadOpPop, nil
	})

	// The same data should be available in the fork,
	// as it was not popped by the read above.
	_ = fork.Read(1, func(items Items) (ReadOp, error) {
		fmt.Println(string(items[0].Blob))
		return ReadOpPop, nil
	})

	// Output:
	// some data
	// some data
}
