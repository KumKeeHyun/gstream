package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/KumKeeHyun/gstream"
)

type User struct {
	id   int
	name string
	age  int
}

var userKeySelector = func(u User) int { return u.id }

func main() {
	builder := gstream.NewBuilder()

	userInput := make(chan User)
	source := gstream.Stream[User](builder).From(userInput)

	half50Users := gstream.SelectKey(source, userKeySelector).
		Filter(func(kv gstream.KeyValue[int, User]) bool {
			return kv.Value.age > 25
		}).Pipe()

	gstream.KVMap(half50Users, func(_ context.Context, kv gstream.KeyValue[int, User]) gstream.KeyValue[string, User] {
		return gstream.NewKeyValue(strconv.Itoa(kv.Key), kv.Value)
	}).Foreach(func(_ context.Context, kv gstream.KeyValue[string, User]) {
		fmt.Println("key:", kv.Key, ", value:", kv.Value)
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		builder.BuildAndStart(ctx)
		done <- struct{}{}
	}()

	userInput <- User{1, "kum", 24}
	userInput <- User{2, "kim", 26}
	userInput <- User{3, "park", 28}
	userInput <- User{4, "sin", 22}

	cancel()
	<-done
}
