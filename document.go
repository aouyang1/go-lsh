package lsh

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

type SimpleDocument struct {
	UID    uint64    `json:"uid"`
	Index  int64     `json:"index"` // represents the first timestamp of the vector
	Vector []float64 `json:"vector"`
}

func NewSimpleDocument(uid uint64, index int64, v []float64) *SimpleDocument {
	return &SimpleDocument{
		UID:    uid,
		Index:  index,
		Vector: v,
	}
}

func (d SimpleDocument) Copy() Document {
	vec := d.GetVector()
	nextVec := make([]float64, len(vec))
	copy(nextVec, vec)
	next := &SimpleDocument{
		UID:    d.GetUID(),
		Index:  d.GetIndex(),
		Vector: nextVec,
	}
	return next
}

func (d SimpleDocument) GetUID() uint64 {
	return d.UID
}

func (d SimpleDocument) GetIndex() int64 {
	return d.Index
}

func (d SimpleDocument) GetVector() []float64 {
	return d.Vector
}

func (d SimpleDocument) Register() {
	gob.Register(d)
}
