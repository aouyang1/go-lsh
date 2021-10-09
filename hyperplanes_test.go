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

	if _, err := NewHyperplanes(5, 0); err != ErrInvalidVectorLength {
		t.Error(err)
		return
	}

	nh := 4
	vl := 7
	h, err := NewHyperplanes(nh, vl)
	if err != nil {
		t.Error(err)
		return
	}
	if len(h.Planes) != nh {
		t.Errorf("expected %d hyperplanes, but got %d", nh, len(h.Planes))
		return
	}
	for _, p := range h.Planes {
		if len(p) != vl {
			t.Errorf("expected %d vector length, but got %d", vl, len(p))
			continue
		}
		vecLen := math.Sqrt(floats.Dot(p, p))
		if vecLen-1.0 > 1e-12 {
			t.Errorf("did not get a unit vector with %v, length, %.3f", p, vecLen)
			continue
		}
	}
}

func TestHyperplaneHash64(t *testing.T) {
	h := &Hyperplanes{
		Planes: [][]float64{
			{0, 0, 1},
			{0, 1, 0},
			{1, 0, 0},
		},
	}
	if _, err := h.Hash64([]float64{}); err != ErrNoVector {
		t.Fatal(err)
	}
	if _, err := h.Hash64([]float64{1, 2}); !strings.Contains(err.Error(), ErrVectorLengthMismatch.Error()) {
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
		hash, err := h.Hash64(td.f)
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

func TestHyperplaneHash32(t *testing.T) {
	h := &Hyperplanes{
		Planes: [][]float64{
			{0, 0, 1},
			{0, 1, 0},
			{1, 0, 0},
		},
	}
	if _, err := h.Hash32([]float64{}); err != ErrNoVector {
		t.Fatal(err)
	}
	if _, err := h.Hash32([]float64{1, 2}); !strings.Contains(err.Error(), ErrVectorLengthMismatch.Error()) {
		t.Fatal(err)
	}

	testData := []struct {
		f    []float64
		hash uint32
	}{
		{[]float64{0, 0, 1}, binary.BigEndian.Uint32([]byte{128, 0, 0, 0})},
		{[]float64{0, 1, 0}, binary.BigEndian.Uint32([]byte{64, 0, 0, 0})},
		{[]float64{1, 0, 0}, binary.BigEndian.Uint32([]byte{32, 0, 0, 0})},
		{[]float64{math.Sqrt(1.0 / 3.0), math.Sqrt(1.0 / 3.0), math.Sqrt(1.0 / 3.0)}, binary.BigEndian.Uint32([]byte{224, 0, 0, 0})},
		{[]float64{-math.Sqrt(1.0 / 3.0), -math.Sqrt(1.0 / 3.0), -math.Sqrt(1.0 / 3.0)}, binary.BigEndian.Uint32([]byte{0, 0, 0, 0})},
		{[]float64{0, 0, -1}, binary.BigEndian.Uint32([]byte{0, 0, 0, 0})},
	}
	for _, td := range testData {
		hash, err := h.Hash32(td.f)
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

func TestHyperplaneHash16(t *testing.T) {
	h := &Hyperplanes{
		Planes: [][]float64{
			{0, 0, 1},
			{0, 1, 0},
			{1, 0, 0},
		},
	}
	if _, err := h.Hash16([]float64{}); err != ErrNoVector {
		t.Fatal(err)
	}
	if _, err := h.Hash16([]float64{1, 2}); !strings.Contains(err.Error(), ErrVectorLengthMismatch.Error()) {
		t.Fatal(err)
	}

	testData := []struct {
		f    []float64
		hash uint16
	}{
		{[]float64{0, 0, 1}, binary.BigEndian.Uint16([]byte{128, 0})},
		{[]float64{0, 1, 0}, binary.BigEndian.Uint16([]byte{64, 0})},
		{[]float64{1, 0, 0}, binary.BigEndian.Uint16([]byte{32, 0})},
		{[]float64{math.Sqrt(1.0 / 3.0), math.Sqrt(1.0 / 3.0), math.Sqrt(1.0 / 3.0)}, binary.BigEndian.Uint16([]byte{224, 0})},
		{[]float64{-math.Sqrt(1.0 / 3.0), -math.Sqrt(1.0 / 3.0), -math.Sqrt(1.0 / 3.0)}, binary.BigEndian.Uint16([]byte{0, 0})},
		{[]float64{0, 0, -1}, binary.BigEndian.Uint16([]byte{0, 0})},
	}
	for _, td := range testData {
		hash, err := h.Hash16(td.f)
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

func TestHyperplaneHash8(t *testing.T) {
	h := &Hyperplanes{
		Planes: [][]float64{
			{0, 0, 1},
			{0, 1, 0},
			{1, 0, 0},
		},
	}
	if _, err := h.Hash8([]float64{}); err != ErrNoVector {
		t.Fatal(err)
	}
	if _, err := h.Hash8([]float64{1, 2}); !strings.Contains(err.Error(), ErrVectorLengthMismatch.Error()) {
		t.Fatal(err)
	}

	testData := []struct {
		f    []float64
		hash uint8
	}{
		{[]float64{0, 0, 1}, uint8(128)},
		{[]float64{0, 1, 0}, uint8(64)},
		{[]float64{1, 0, 0}, uint8(32)},
		{[]float64{math.Sqrt(1.0 / 3.0), math.Sqrt(1.0 / 3.0), math.Sqrt(1.0 / 3.0)}, uint8(224)},
		{[]float64{-math.Sqrt(1.0 / 3.0), -math.Sqrt(1.0 / 3.0), -math.Sqrt(1.0 / 3.0)}, uint8(0)},
		{[]float64{0, 0, -1}, uint8(0)},
	}
	for _, td := range testData {
		hash, err := h.Hash8(td.f)
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

func BenchmarkHyperplaneNew(b *testing.B) {
	numHyperplanes := 8
	vecLen := 60

	for i := 0; i < b.N; i++ {
		_, err := NewHyperplanes(numHyperplanes, vecLen)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHyperplaneHash64(b *testing.B) {
	numHyperplanes := 8
	vecLen := 60

	h, err := NewHyperplanes(numHyperplanes, vecLen)
	if err != nil {
		b.Fatal(err)
	}

	v := make([]float64, vecLen)
	for i := 0; i < b.N; i++ {
		_, err := h.Hash64(v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHyperplaneHash32(b *testing.B) {
	numHyperplanes := 8
	vecLen := 60

	h, err := NewHyperplanes(numHyperplanes, vecLen)
	if err != nil {
		b.Fatal(err)
	}

	v := make([]float64, vecLen)
	for i := 0; i < b.N; i++ {
		_, err := h.Hash32(v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHyperplaneHash16(b *testing.B) {
	numHyperplanes := 8
	vecLen := 60

	h, err := NewHyperplanes(numHyperplanes, vecLen)
	if err != nil {
		b.Fatal(err)
	}

	v := make([]float64, vecLen)
	for i := 0; i < b.N; i++ {
		_, err := h.Hash16(v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHyperplaneHash8(b *testing.B) {
	numHyperplanes := 8
	vecLen := 60

	h, err := NewHyperplanes(numHyperplanes, vecLen)
	if err != nil {
		b.Fatal(err)
	}

	v := make([]float64, vecLen)
	for i := 0; i < b.N; i++ {
		_, err := h.Hash8(v)
		if err != nil {
			b.Fatal(err)
		}
	}
}
