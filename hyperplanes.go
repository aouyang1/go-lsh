package lsh

import (
	"encoding/binary"
	"fmt"
	"math/rand"

	"gonum.org/v1/gonum/floats"
)

type Hyperplanes struct {
	Planes [][]float64
	Buffer []byte
}

func NewHyperplanes(numHyperplanes, numFeatures int) (*Hyperplanes, error) {
	if numHyperplanes < 1 {
		return nil, ErrInvalidNumHyperplanes
	}

	if numFeatures < 1 {
		return nil, ErrInvalidNumFeatures
	}

	h := new(Hyperplanes)
	h.Buffer = make([]byte, 8)
	h.Planes = make([][]float64, numHyperplanes)
	for i := 0; i < numHyperplanes; i++ {
		h.Planes[i] = make([]float64, numFeatures)
		for j := 0; j < numFeatures; j++ {
			h.Planes[i][j] = rand.Float64() - 0.5
		}
		floats.Scale(1/floats.Norm(h.Planes[i], 2), h.Planes[i])
	}

	return h, nil
}

func (h *Hyperplanes) hash(f []float64) (uint64, error) {
	if len(f) == 0 {
		return 0, ErrNoFeatures
	}

	bs := h.Buffer
	var b byte
	var bitCnt, byteCnt int

	for _, p := range h.Planes {
		if len(f) != len(p) {
			return 0, fmt.Errorf("%v, has length %d when expecting length, %d", ErrFeatureLengthMismatch, len(f), len(p))
		}
		if floats.Dot(p, f) > 0 {
			b = b | byte(1)<<(8-bitCnt-1)
		}
		bitCnt++
		if bitCnt == 8 {
			bs[byteCnt] = b
			bitCnt = 0
			b = 0
			byteCnt++
		}
	}

	// didn't fill a full byte
	if bitCnt != 0 {
		bs[byteCnt] = b
	}
	return binary.BigEndian.Uint64(bs), nil
}
