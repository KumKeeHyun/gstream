package main

import (
	"fmt"

	"github.com/KumKeeHyun/gstream"
)

type UserName struct {
	id   int
	name string
}

var userNameSelectKey = func(u UserName) int { return u.id }

type UserAge struct {
	id  int
	age int
}

var userAgeSelectKey = func(u UserAge) int { return u.id }

type User struct {
	id   int
	name string
	age  int
}

var userJoiner = func(un UserName, ua UserAge) User {
	return User{
		id:   un.id,
		name: un.name,
		age:  ua.age,
	}
}

func main() {
	nameInput := make(chan UserName)
	ageInput := make(chan UserAge)
	
	builder := gstream.NewBuilder()

	ageTable := gstream.Table[int, UserAge](builder).
		From(ageInput, userAgeSelectKey, gstream.IntSerde)
	nameStream := gstream.Stream[UserName](builder).
		From(nameInput)

	keyedNameStream := gstream.SelectKey(nameStream, userNameSelectKey)
	gstream.Joined[int, UserName, UserAge, User](keyedNameStream).
		JoinTable(ageTable, userJoiner).
		ToStream().
		Foreach(func(u User) {
			fmt.Println(u)
		})

	close := builder.BuildAndStart()

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

	close()
}
