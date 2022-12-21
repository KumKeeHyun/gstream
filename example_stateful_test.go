package gstream

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/gstream/state"
	"sort"
)

type Player struct {
	ID   int
	Name string
}

type Product struct {
	ID   int
	Name string
}

type ScoreEvent struct {
	PlayerID  int
	ProductID int
	Score     float64
}

type ScoreWithPlayer struct {
	Player Player
	Score  ScoreEvent
}

type Enriched struct {
	PlayerID    int
	PlayerName  string
	ProductID   int
	ProductName string
	Score       float64
}

type HighScores struct {
	highScores []Enriched
}

func (s *HighScores) Add(e Enriched) {
	added := append(s.highScores, e)
	sort.SliceStable(added, func(i, j int) bool {
		return added[i].Score > added[j].Score
	})
	if len(added) > 3 {
		added = added[:3]
	}
	s.highScores = added
}

// ExampleStateful demonstrates a stateful processing using table, join, aggregate.
func Example_stateful() {
	playerCh := make(chan Player)
	productCh := make(chan Product)
	scoreCh := make(chan ScoreEvent)

	// Create producer emitting players, products, scores
	go func() {
		defer func() {
			close(playerCh)
			close(productCh)
			close(scoreCh)
		}()

		for i := 1; i <= 3; i++ {
			playerCh <- Player{
				ID:   i,
				Name: fmt.Sprintf("player-%d", i),
			}
		}
		for i := 1; i <= 2; i++ {
			productCh <- Product{
				ID:   i,
				Name: fmt.Sprintf("product-%d", i),
			}
		}

		scoreCh <- ScoreEvent{
			PlayerID:  1,
			ProductID: 1,
			Score:     0.6,
		}
		scoreCh <- ScoreEvent{
			PlayerID:  2,
			ProductID: 1,
			Score:     0.5,
		}
		scoreCh <- ScoreEvent{
			PlayerID:  3,
			ProductID: 1,
			Score:     0.7,
		}
		scoreCh <- ScoreEvent{
			PlayerID:  2,
			ProductID: 2,
			Score:     0.8,
		}
		scoreCh <- ScoreEvent{
			PlayerID:  1,
			ProductID: 1,
			Score:     0.8,
		}
		scoreCh <- ScoreEvent{
			PlayerID:  3,
			ProductID: 2,
			Score:     0.4,
		}
		scoreCh <- ScoreEvent{
			PlayerID:  2,
			ProductID: 1,
			Score:     0.9,
		}
	}()

	b := NewBuilder()

	players := Table[int, Player](b).From(
		playerCh,
		func(p Player) int { return p.ID },
		state.NewOptions[int, Player](),
	)
	products := Table[int, Product](b).From(
		productCh,
		func(p Product) int { return p.ID },
		state.NewOptions[int, Product](),
	)
	scores := SelectKey[int, ScoreEvent](
		Stream[ScoreEvent](b).From(scoreCh),
		func(s ScoreEvent) int { return s.PlayerID },
	)

	// ScoreEvent join with player by playerID
	withPlayers := JoinStreamTable(
		scores,
		players,
		func(id int, score ScoreEvent, player Player) ScoreWithPlayer {
			return ScoreWithPlayer{
				Player: player,
				Score:  score,
			}
		},
	)
	// Group by productID
	groupByProduct := GroupBy(
		withPlayers,
		func(playerID int, withPlayer ScoreWithPlayer) int {
			return withPlayer.Score.ProductID
		},
	)
	// withPlayer join with product by productID
	withProducts := JoinStreamTable(
		groupByProduct,
		products,
		func(playerID int, withPlayer ScoreWithPlayer, product Product) Enriched {
			return Enriched{
				PlayerID:    withPlayer.Player.ID,
				PlayerName:  withPlayer.Player.Name,
				ProductID:   product.ID,
				ProductName: product.Name,
				Score:       withPlayer.Score.Score,
			}
		},
	)

	// Perform the aggregation
	Aggregate[int, Enriched, *HighScores](
		withProducts,
		func() *HighScores {
			return &HighScores{highScores: []Enriched{}}
		},
		func(kv KeyValue[int, Enriched], aggregate *HighScores) *HighScores {
			aggregate.Add(kv.Value)
			return aggregate
		},
		state.NewOptions[int, *HighScores](),
	).
		ToValueStream().
		Foreach(func(_ context.Context, hs *HighScores) {
			fmt.Println(hs.highScores)
		})

	b.BuildAndStart(context.Background())

	// Output:
	// [{1 player-1 1 product-1 0.6}]
	// [{1 player-1 1 product-1 0.6} {2 player-2 1 product-1 0.5}]
	// [{3 player-3 1 product-1 0.7} {1 player-1 1 product-1 0.6} {2 player-2 1 product-1 0.5}]
	// [{2 player-2 2 product-2 0.8}]
	// [{1 player-1 1 product-1 0.8} {3 player-3 1 product-1 0.7} {1 player-1 1 product-1 0.6}]
	// [{2 player-2 2 product-2 0.8} {3 player-3 2 product-2 0.4}]
	// [{2 player-2 1 product-1 0.9} {1 player-1 1 product-1 0.8} {3 player-3 1 product-1 0.7}]
}
