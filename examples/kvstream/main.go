package main

import (
	"fmt"

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
	gstream.SelectKey(source, userKeySelector).
		Filter(func(kv gstream.KeyValue[int, User]) bool {
			return kv.Value.age > 25
		}).Pipe().Foreach(func(kv gstream.KeyValue[int, User]) {
			fmt.Println(kv.Value)
		})

	close := builder.BuildAndStart()

	userInput <- User{1, "kum", 24}
	userInput <- User{2, "kim", 26}
	userInput <- User{3, "park", 28}
	userInput <- User{4, "sin", 22}
	close()
}