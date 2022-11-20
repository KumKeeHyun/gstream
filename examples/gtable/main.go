package main

import (
	"fmt"

	"github.com/KumKeeHyun/gstream"
	"github.com/KumKeeHyun/gstream/materialized"
)

type User struct {
	id   int
	name string
}

var userSelectKey = func(u User) int { return u.id }

func main() {
	input := make(chan User)

	builder := gstream.NewBuilder()
	source := gstream.Stream[User](builder).From(input)

	userMaterialized := materialized.New(
		materialized.WithKeySerde[int, User](gstream.IntSerde),
		materialized.WithInMemory[int, User](),
	)
	gstream.SelectKey(source, userSelectKey).
		ToTable(userMaterialized).
		ToValueStream().
		Foreach(func(u User) {
			fmt.Println(u)
		})

	close := builder.BuildAndStart()

	input <- User{1, "kum"}
	input <- User{2, "kim"}
	input <- User{3, "park"}
	input <- User{1, "kuem"}
	input <- User{4, "lee"}
	close()
}
