package lsh

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"

	"gonum.org/v1/gonum/floats"
)

var (
	ErrNumHyperplanesExceedHashBits = errors.New("number of hyperplanes exceeds available bits to encode vector")
)

// Hyperplanes is composed of a number of randomly generated unit vectors where the vector length is based on the
// configured vector length it is to represent.
type Hyperplanes struct {
	Planes [][]float64
}

func NewHyperplanes(numHyperplanes, vecLen int) (*Hyperplanes, error) {
	if numHyperplanes < 1 {
		return nil, ErrInvalidNumHyperplanes
	}

	if vecLen < 1 {
		return nil, ErrInvalidVectorLength
	}

	h := new(Hyperplanes)
	h.Planes = make([][]float64, numHyperplanes)
	for i := 0; i < numHyperplanes; i++ {
		h.Planes[i] = make([]float64, vecLen)
		for j := 0; j < vecLen; j++ {
			h.Planes[i][j] = rand.Float64() - 0.5
		}
		floats.Scale(1/floats.Norm(h.Planes[i], 2), h.Planes[i])
	}

	return h, nil
}

func (h *Hyperplanes) Hash64(f []float64) (uint64, error) {
	if len(f) == 0 {
		return 0, ErrNoVector
	}
	if len(h.Planes) > 64 {
		return 0, ErrNumHyperplanesExceedHashBits
	}
	buffer := make([]byte, 8)
	if err := h.hash(f, buffer); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buffer), nil
}

func (h *Hyperplanes) Hash32(f []float64) (uint32, error) {
	if len(f) == 0 {
		return 0, ErrNoVector
	}
	if len(h.Planes) > 32 {
		return 0, ErrNumHyperplanesExceedHashBits
	}
	buffer := make([]byte, 4)
	if err := h.hash(f, buffer); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buffer), nil
}

func (h *Hyperplanes) Hash16(f []float64) (uint16, error) {
	if len(f) == 0 {
		return 0, ErrNoVector
	}
	if len(h.Planes) > 16 {
		return 0, ErrNumHyperplanesExceedHashBits
	}
	buffer := make([]byte, 2)
	if err := h.hash(f, buffer); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(buffer), nil
}

func (h *Hyperplanes) Hash8(f []float64) (uint8, error) {
	if len(f) == 0 {
		return 0, ErrNoVector
	}
	if len(h.Planes) > 8 {
		return 0, ErrNumHyperplanesExceedHashBits
	}
	buffer := make([]byte, 1)
	if err := h.hash(f, buffer); err != nil {
		return 0, err
	}
	return buffer[0], nil
}

func (h *Hyperplanes) hash(f []float64, buffer []byte) error {
	var b byte
	var bitCnt, byteCnt int

	for _, p := range h.Planes {
		if len(f) != len(p) {
			return fmt.Errorf("%v, has length %d when expecting length, %d", ErrVectorLengthMismatch, len(f), len(p))
		}
		if floats.Dot(p, f) > 0 {
			b = b | byte(1)<<(8-bitCnt-1)
		}
		bitCnt++
		if bitCnt == 8 {
			buffer[byteCnt] = b
			bitCnt = 0
			b = 0
			byteCnt++
		}
	}

	// didn't fill a full byte
	if bitCnt != 0 {
		buffer[byteCnt] = b
	}
	return nil
}
