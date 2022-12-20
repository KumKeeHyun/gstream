package main

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/gstream"
	"github.com/KumKeeHyun/gstream/state"
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

	uopt := state.NewOptions(
		state.WithInMemory[int, User](),
	)
	gstream.SelectKey(source, userSelectKey).
		ToTable(uopt).
		ToValueStream().
		Foreach(func(_ context.Context, u User) {
			fmt.Println(u)
		})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		builder.BuildAndStart(ctx)
		done <- struct{}{}
	}()

	input <- User{1, "kum"}
	input <- User{2, "kim"}
	input <- User{3, "park"}
	input <- User{1, "kuem"}
	input <- User{4, "lee"}

	cancel()
	<-done
}
