package timeq

import (
	"fmt"
	"os"
	"reflect"
)

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
	_ = queue.Read(10, func(_ Transaction, popItems Items) (ReadOp, error) {
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

func ExampleQueue_Fork() {
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
	_ = queue.Read(1, func(_ Transaction, items Items) (ReadOp, error) {
		fmt.Println(string(items[0].Blob))
		return ReadOpPop, nil
	})

	// The same data should be available in the fork,
	// as it was not popped by the read above.
	_ = fork.Read(1, func(_ Transaction, items Items) (ReadOp, error) {
		fmt.Println(string(items[0].Blob))
		return ReadOpPop, nil
	})

	// Output:
	// some data
	// some data
}

func ExampleTransaction() {
	// Error handling stripped for brevity:
	dir, _ := os.MkdirTemp("", "timeq-example")
	defer os.RemoveAll(dir)

	// Open the queue. If it does not exist, it gets created:
	queue, _ := Open(dir, DefaultOptions())

	_ = queue.Push(Items{
		Item{
			Key:  123,
			Blob: []byte("some data"),
		},
		Item{
			Key:  456,
			Blob: []byte("other data"),
		},
	})

	_ = queue.Read(1, func(tx Transaction, items Items) (ReadOp, error) {
		// Push half of the data back to the queue.
		// You can use that to "unread" parts of what you read.
		return ReadOpPop, tx.Push(items[1:])
	})

	fmt.Println(queue.Len())

	// Output:
	// 1
}

func ExamplePopCopy() {
	// Error handling stripped for brevity:
	dir, _ := os.MkdirTemp("", "timeq-example")
	defer os.RemoveAll(dir)

	// Open the queue. If it does not exist, it gets created:
	queue, _ := Open(dir, DefaultOptions())

	items := make(Items, 0, 5)
	for idx := 0; idx < 10; idx++ {
		items = append(items, Item{
			Key:  Key(idx),
			Blob: []byte(fmt.Sprintf("%d", idx)),
		})
	}

	_ = queue.Push(items)
	got, _ := PopCopy(queue, 5)
	for _, item := range got {
		fmt.Println(item.Key)
	}

	// Output:
	// K00000000000000000000
	// K00000000000000000001
	// K00000000000000000002
	// K00000000000000000003
	// K00000000000000000004
}
