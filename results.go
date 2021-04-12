package lsh

import (
	"container/heap"
	"math"
)

type Results struct {
	TopN       int
	Threshold  float64
	SignFilter SignFilter
	scores     Scores
}

type SignFilter int

const (
	SignFilter_POS = 1
	SignFilter_NEG = -1
	SignFilter_ANY = 0
)

// NewResults creates a new instance of results to track these similar features
func NewResults(topN int, threshold float64, signFilter SignFilter) *Results {
	scores := make(Scores, 0, topN)

	// Build priority queue of size TopN so that we don't have to sort over the entire
	// score output
	heap.Init(&scores)

	return &Results{
		TopN:       topN,
		Threshold:  threshold,
		SignFilter: signFilter,
		scores:     scores,
	}
}

// passed checks if the input score satisfies the Results lag and threshold requirements
func (r *Results) passed(s Score) bool {
	return math.Abs(float64(s.Score)) >= r.Threshold &&
		(r.SignFilter == SignFilter_ANY ||
			(s.Score > 0 && r.SignFilter == SignFilter_POS) ||
			(s.Score < 0 && r.SignFilter == SignFilter_NEG))
}

// Update records the input score
func (r *Results) Update(s Score) {
	if !r.passed(s) {
		return
	}
	if r.scores.Len() == r.TopN {
		if math.Abs(s.Score) > math.Abs(r.scores[0].Score) {
			heap.Pop(&r.scores)
			heap.Push(&r.scores, s)
		}
	} else {
		heap.Push(&r.scores, s)
	}
}

// Fetch returns the sorted scores in ascending order
func (r *Results) Fetch() Scores {
	s := make(Scores, len(r.scores))
	var score Score
	numScores := len(r.scores)

	for i := numScores - 1; i >= 0; i-- {
		score = heap.Pop(&r.scores).(Score)
		s[i] = score
	}
	return s
}

// Scores is a slice of individual Score's
type Scores []Score

func (s Scores) Len() int {
	return len(s)
}

func (s Scores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Scores) Less(i, j int) bool {
	return math.Abs(s[i].Score) < math.Abs(s[j].Score)
}

// Push implements the function in the heap interface
func (s *Scores) Push(x interface{}) {
	*s = append(*s, x.(Score))
}

// Pop implements the function in the heap interface
func (s *Scores) Pop() interface{} {
	x := (*s)[len(*s)-1]
	*s = (*s)[:len(*s)-1]
	return x
}

func (s Scores) UIDs() []uint64 {
	out := make([]uint64, 0, len(s))
	for _, score := range s {
		out = append(out, score.UID)
	}
	return out
}

func (s Scores) Scores() []float64 {
	out := make([]float64, 0, len(s))
	for _, score := range s {
		out = append(out, score.Score)
	}
	return out
}

type Score struct {
	UID   uint64  `json:"uid"`
	Score float64 `json:"score"`
}
