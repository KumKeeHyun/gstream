package main

import (
	"fmt"

	"github.com/KumKeeHyun/gstream"
	"github.com/KumKeeHyun/gstream/materialized"
)

type User struct {
	id   int
	name string
	age  int
}

var userKeySelector = func(u User) int { return u.id }

type UserHistory struct {
	log []User
}

var initializer = func() *UserHistory {
	return &UserHistory{
		log: make([]User, 0, 10),
	}
}

var aggregator = func(kv gstream.KeyValue[int, User], uh *UserHistory) *UserHistory {
	uh.log = append(uh.log, kv.Value)
	return uh
}

func main() {
	builder := gstream.NewBuilder()

	input := make(chan User)
	source := gstream.Stream[User](builder).From(input)
	users := gstream.SelectKey(source, userKeySelector)

	userMaterialized := materialized.New(
		materialized.WithKeySerde[int, *UserHistory](gstream.IntSerde),
		materialized.WithInMemory[int, *UserHistory](),
	)
	gstream.Aggreate[int, User, *UserHistory](users, initializer, aggregator, userMaterialized).
		ToValueStream().
		Foreach(func(uh *UserHistory) {
			fmt.Println(uh)
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
