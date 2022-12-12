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
		Filter(func(_ int, v User) bool {
			return v.age > 25
		}).Pipe()

	gstream.KeyValueMap(half50Users, func(_ context.Context, k int, v User) (string, User) {
		return strconv.Itoa(k), v
	}).Foreach(func(_ context.Context, k string, v User) {
		fmt.Println("key:", k, ", value:", v)
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
