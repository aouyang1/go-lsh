package lsh

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"gonum.org/v1/gonum/floats"
)

func TestNewHyperplanes(t *testing.T) {
	if _, err := NewHyperplanes(0, 7); err != ErrInvalidNumHyperplanes {
		t.Error(err)
		return
	}

	if _, err := NewHyperplanes(5, 0); err != ErrInvalidNumFeatures {
		t.Error(err)
		return
	}

	nh := 4
	nf := 7
	h, err := NewHyperplanes(nh, nf)
	if err != nil {
		t.Error(err)
		return
	}
	if len(h.Planes) != nh {
		t.Errorf("expected %d hyperplanes, but got %d", nh, len(h.Planes))
		return
	}
	for _, p := range h.Planes {
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

func TestHyperplaneHash(t *testing.T) {
	h := &Hyperplanes{
		Planes: [][]float64{
			{0, 0, 1},
			{0, 1, 0},
			{1, 0, 0},
		},
		Buffer: make([]byte, 8),
	}
	if _, err := h.hash([]float64{}); err != ErrNoFeatures {
		t.Fatal(err)
	}
	if _, err := h.hash([]float64{1, 2}); !strings.Contains(err.Error(), ErrFeatureLengthMismatch.Error()) {
		t.Fatal(err)
	}

	testData := []struct {
		f    []float64
		hash uint64
	}{
		{[]float64{0, 0, 1}, binary.BigEndian.Uint64([]byte{128, 0, 0, 0, 0, 0, 0, 0})},
		{[]float64{0, 1, 0}, binary.BigEndian.Uint64([]byte{64, 0, 0, 0, 0, 0, 0, 0})},
		{[]float64{1, 0, 0}, binary.BigEndian.Uint64([]byte{32, 0, 0, 0, 0, 0, 0, 0})},
		{[]float64{math.Sqrt(1.0 / 3.0), math.Sqrt(1.0 / 3.0), math.Sqrt(1.0 / 3.0)}, binary.BigEndian.Uint64([]byte{224, 0, 0, 0, 0, 0, 0, 0})},
		{[]float64{-math.Sqrt(1.0 / 3.0), -math.Sqrt(1.0 / 3.0), -math.Sqrt(1.0 / 3.0)}, binary.BigEndian.Uint64([]byte{0, 0, 0, 0, 0, 0, 0, 0})},
		{[]float64{0, 0, -1}, binary.BigEndian.Uint64([]byte{0, 0, 0, 0, 0, 0, 0, 0})},
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

	h, err := NewHyperplanes(numHyperplanes, numFeatures)
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
