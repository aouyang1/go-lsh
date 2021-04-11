package lsh

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"gonum.org/v1/gonum/floats"
)

func TestNewOptions(t *testing.T) {
	testData := []struct {
		nf int
		nh int
		nt int

		err error
	}{
		{1, 1, 1, nil},
		{3, 5, 2, nil},
		{0, 0, 0, errInvalidNumHyperplanes},
		{3, 65, 2, errExceededMaxNumHyperplanes},
		{0, 5, 2, errInvalidNumFeatures},
		{3, 5, 0, errInvalidNumTables},
	}
	for _, td := range testData {
		opt := &Options{td.nh, td.nt, td.nf}
		if err := opt.Validate(); err != td.err {
			t.Errorf("expected %v, but got %v", td.err, err)
			continue
		}
	}
}

func TestNewHyperplanes(t *testing.T) {
	nf := 7
	nh := 4
	h, err := newHyperplanes(nh, nf)
	if err != nil {
		t.Error(err)
		return
	}
	if len(h.planes) != nh {
		t.Errorf("expected %d hyperplanes, but got %d", nh, len(h.planes))
		return
	}
	for _, p := range h.planes {
		if len(p) != nf {
			t.Errorf("expected %d features, but got %d", nf, len(p))
			continue
		}
		vecLen := math.Sqrt(floats.Dot(p, p))
		if vecLen-1.0 > 1e-12 {
			t.Errorf("did not get a unit vector with %v, length, %.3f", p, vecLen)
			continue
		}
	}
}

func TestNewLSH(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := NewLSH(opt)
	if err != nil {
		t.Fatal(err)
	}
	if len(lsh.tables) != opt.NumTables {
		t.Fatalf("expected %d tables, but got %d", opt.NumTables, len(lsh.tables))
	}
}

func TestLSH(t *testing.T) {
	opt := NewDefaultOptions()
	lsh, err := NewLSH(opt)
	if err != nil {
		t.Fatal(err)
	}

	docs := []*Document{
		{0, []float64{0, 0, 5}},
		{1, []float64{0, 0.1, 3}},
		{2, []float64{0, 0.1, 2}},
		{3, []float64{0, 0.1, 1}},
	}
	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	scores, err := lsh.Search([]float64{0, 0, 0.1}, 3, 0.65)
	if err != nil {
		t.Fatal(err)
	}
	expected := []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	if err := lsh.Delete(2); err != nil {
		t.Fatal(err)
	}

	scores, err = lsh.Search([]float64{0, 0, 0.1}, 3, 0.65)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 3}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	if err := lsh.Index(NewDocument(2, []float64{0, 0.1, 2})); err != nil {
		t.Fatal(err)
	}
	scores, err = lsh.Search([]float64{0, 0, 0.1}, 3, 0.65)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

}
func TestHyperplaneHash(t *testing.T) {
	testData := []struct {
		f    []float64
		hash uint64
	}{
		{[]float64{0, 0, 1}, binary.BigEndian.Uint64([]byte{128, 0, 0, 0, 0, 0, 0, 0})},
		{[]float64{0, 1, 0}, binary.BigEndian.Uint64([]byte{64, 0, 0, 0, 0, 0, 0, 0})},
		{[]float64{1, 0, 0}, binary.BigEndian.Uint64([]byte{32, 0, 0, 0, 0, 0, 0, 0})},
		{[]float64{math.Sqrt(1.0 / 3.0), math.Sqrt(1.0 / 3.0), math.Sqrt(1.0 / 3.0)}, binary.BigEndian.Uint64([]byte{224, 0, 0, 0, 0, 0, 0, 0})},
		{[]float64{-math.Sqrt(1.0 / 3.0), -math.Sqrt(1.0 / 3.0), -math.Sqrt(1.0 / 3.0)}, binary.BigEndian.Uint64([]byte{0, 0, 0, 0, 0, 0, 0, 0})},
	}
	h := &hyperplanes{
		planes: [][]float64{
			{0, 0, 1},
			{0, 1, 0},
			{1, 0, 0},
		},
		buffer: make([]byte, 8),
	}

	for _, td := range testData {
		hash, err := h.hash(td.f)
		if err != nil {
			t.Error(err)
			continue
		}
		if hash != td.hash {
			t.Errorf("expected %d, but got %d", td.hash, hash)
			continue
		}
	}
}

func BenchmarkHyperplanHash(b *testing.B) {
	numHyperplanes := 16
	numFeatures := 60

	h, err := newHyperplanes(numHyperplanes, numFeatures)
	if err != nil {
		b.Fatal(err)
	}

	features := make([]float64, numFeatures)
	for i := 0; i < b.N; i++ {
		_, err := h.hash(features)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func compareUint64s(expected, uids []uint64) error {
	if len(uids) != len(expected) {
		return fmt.Errorf("expected %d results, but got %d", len(expected), len(uids))
	}
	for i, uid := range uids {
		if uid != expected[i] {
			return fmt.Errorf("expected %v, but got %v", expected, uids)
		}
	}
	return nil
}
