package main

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/gstream"
	"github.com/KumKeeHyun/gstream/state"
)

type UserName struct {
	id   int
	name string
}

var nameKeySelector = func(u UserName) int { return u.id }

type UserAge struct {
	id  int
	age int
}

var ageKeySelector = func(u UserAge) int { return u.id }

type User struct {
	id   int
	name string
	age  int
}

var userJoiner = func(id int, un UserName, ua UserAge) User {
	return User{
		id:   id,
		name: un.name,
		age:  ua.age,
	}
}

func main() {
	nameInput := make(chan UserName)
	ageInput := make(chan UserAge)

	builder := gstream.NewBuilder()

	sopt := state.NewOptions(
		state.WithInMemory[int, UserAge](),
	)
	ageTable := gstream.Table[int, UserAge](builder).
		From(ageInput, ageKeySelector, sopt)

	nameStream := gstream.Stream[UserName](builder).
		From(nameInput)

	keyedNameStream := gstream.SelectKey(nameStream, nameKeySelector)
	gstream.JoinStreamTable(keyedNameStream, ageTable, userJoiner).
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

	ageInput <- UserAge{1, 24}
	ageInput <- UserAge{2, 26}
	ageInput <- UserAge{4, 22}

	nameInput <- UserName{1, "kum"}
	nameInput <- UserName{2, "kim"}
	nameInput <- UserName{3, "park"}
	nameInput <- UserName{1, "kuem"}
	nameInput <- UserName{4, "sin"}

	ageInput <- UserAge{3, 28}
	nameInput <- UserName{3, "park"}

	cancel()
	<-done
}
