package gstream

import (
	"context"
	"fmt"
)

type Tweet struct {
	ID   int
	Lang string
	Text string
}

type Sentiment struct {
	ID    int
	Text  string
	Score float64
}

// ExampleStateful demonstrates a stateless processing using stream, filter, map, merge
func Example_stateless() {
	tweetCh := make(chan Tweet)

	// Create producer emitting tweets
	go func() {
		defer close(tweetCh)
		for i := 0; i < 3; i++ {
			tweetCh <- Tweet{
				ID:   i,
				Lang: "en",
				Text: fmt.Sprintf("some text %d", i),
			}
			tweetCh <- Tweet{
				ID:   i + 10,
				Lang: "kr",
				Text: fmt.Sprintf("썸 텍스트 %d", i),
			}
		}
	}()

	b := NewBuilder()
	tweets := Stream[Tweet](b).From(tweetCh)

	// Branch into english
	english := tweets.Filter(func(t Tweet) bool {
		return t.Lang == "en"
	})

	// Branch into korean and translate
	translate := tweets.Filter(func(t Tweet) bool {
		return t.Lang == "kr"
	}).Map(func(ctx context.Context, t Tweet) Tweet {
		// Translate t.Text to English
		return Tweet{
			ID:   t.ID,
			Lang: "en",
			Text: fmt.Sprintf("translated text %d", t.ID),
		}
	})

	// Merge english and translate branch
	merged := english.Merge(translate)

	// Enrich tweet
	sentiment := FlatMap(merged, func(ctx context.Context, t Tweet) []Sentiment {
		// Calculate sentiment score of t.Text and enrich tweet
		return []Sentiment{
			{
				ID:    t.ID,
				Text:  t.Text,
				Score: 0.5,
			},
		}
	})

	// Print sentiment
	sentiment.Foreach(func(_ context.Context, s Sentiment) {
		fmt.Println(s)
	})

	b.BuildAndStart(context.Background())

	// Output:
	// {0 some text 0 0.5}
	// {10 translated text 10 0.5}
	// {1 some text 1 0.5}
	// {11 translated text 11 0.5}
	// {2 some text 2 0.5}
	// {12 translated text 12 0.5}
}
