package main

import (
	"fmt"
	"github.com/KumKeeHyun/gstream"
	"github.com/KumKeeHyun/gstream/materialized"
	"log"
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

	//userMater, err := materialized.New(
	//	materialized.WithInMemory[int, *UserHistory](),
	//)
	userMater, err := materialized.New(
		materialized.WithBoltDB[int, *UserHistory]("userhistory"),
	)
	if err != nil {
		log.Fatal(err)
	}
	gstream.Aggregate[int, User, *UserHistory](users, initializer, aggregator, userMater).
		ToValueStream().
		Foreach(func(uh *UserHistory) {
			fmt.Println(uh)
		})

	countMater, err := materialized.New(
		materialized.WithBoltDB[int, int]("countuser"),
	)
	if err != nil {
		log.Fatal(err)
	}
	gstream.Count(users, countMater).
		ToStream().
		Foreach(func(k, v int) {
			fmt.Println(k, v)
		})
	close := builder.BuildAndStart()

	input <- User{1, "kum", 22}
	input <- User{2, "kim", 24}
	input <- User{1, "kum", 23}
	input <- User{2, "kim", 25}
	input <- User{1, "kum", 26}
	input <- User{2, "kim", 28}

	close()
}
