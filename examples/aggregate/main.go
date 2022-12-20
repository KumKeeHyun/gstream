package main

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/gstream"
	"github.com/KumKeeHyun/gstream/state"
)

type User struct {
	ID   int
	Name string
	Age  int
}

var userKeySelector = func(u User) int { return u.ID }

type UserHistory struct {
	Log []User
}

var initializer = func() *UserHistory {
	return &UserHistory{
		Log: make([]User, 0, 10),
	}
}

var aggregator = func(kv gstream.KeyValue[int, User], uh *UserHistory) *UserHistory {
	uh.Log = append(uh.Log, kv.Value)
	return uh
}

func main() {
	builder := gstream.NewBuilder()

	input := make(chan User)
	source := gstream.Stream[User](builder).From(input)
	users := gstream.SelectKey(source, userKeySelector)

	hopt := state.NewOptions(
		state.WithBoltDB[int, *UserHistory]("userhistory"),
	)
	gstream.Aggregate[int, User, *UserHistory](users, initializer, aggregator, hopt).
		ToValueStream().
		Foreach(func(_ context.Context, uh *UserHistory) {
			fmt.Println(uh)
		})

	copt := state.NewOptions(
		state.WithBoltDB[int, int]("countuser"),
	)
	gstream.Count(users, copt).
		ToStream().
		Foreach(func(_ context.Context, kv gstream.KeyValue[int, int]) {
			fmt.Println(kv.Key, kv.Value)
		})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		builder.BuildAndStart(ctx)
		done <- struct{}{}
	}()

	input <- User{1, "kum", 22}
	input <- User{2, "kim", 24}
	input <- User{1, "kum", 23}
	input <- User{2, "kim", 25}
	input <- User{1, "kum", 26}
	input <- User{2, "kim", 28}

	cancel()
	<-done

}
