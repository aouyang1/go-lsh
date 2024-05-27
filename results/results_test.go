package results

import (
	"testing"
)

func TestScores(t *testing.T) {
	s := Scores{
		{0, 0.9},
		{1, 0.8},
		{2, 0.7},
	}
	res := s.Scores()
	expected := []float64{0.9, 0.8, 0.7}
	if len(res) != len(expected) {
		t.Fatalf("expected %d scores, but got %d", len(expected), len(res))
	}
	for i, score := range res {
		if score != expected[i] {
			t.Fatalf("expected score value %.2f, but got %.2f", expected[i], score)
		}
	}
}
