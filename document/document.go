package document

import (
	"encoding/gob"
)

type Document interface {
	Copy() Document
	GetUID() uint64
	GetIndex() int64
	GetVector() []float64
	Register()
}

type Simple struct {
	UID    uint64    `json:"uid"`
	Index  int64     `json:"index"` // represents the first timestamp of the vector
	Vector []float64 `json:"vector"`
}

func NewSimple(uid uint64, index int64, v []float64) *Simple {
	return &Simple{
		UID:    uid,
		Index:  index,
		Vector: v,
	}
}

func (s Simple) Copy() Document {
	vec := s.GetVector()
	nextVec := make([]float64, len(vec))
	copy(nextVec, vec)
	next := &Simple{
		UID:    s.GetUID(),
		Index:  s.GetIndex(),
		Vector: nextVec,
	}
	return next
}

func (s Simple) GetUID() uint64 {
	return s.UID
}

func (s Simple) GetIndex() int64 {
	return s.Index
}

func (s Simple) GetVector() []float64 {
	return s.Vector
}

func (s Simple) Register() {
	gob.Register(s)
}
