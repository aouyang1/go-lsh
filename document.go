package lsh

import (
	"encoding/gob"
)

type Document interface {
	GetUID() uint64
	GetVector() []float64
	Register()
}

type SimpleDocument struct {
	UID    uint64    `json:"uid"`
	Vector []float64 `json:"vector"`
}

func NewSimpleDocument(uid uint64, v []float64) *SimpleDocument {
	return &SimpleDocument{
		UID:    uid,
		Vector: v,
	}
}

func (d SimpleDocument) GetUID() uint64 {
	return d.UID
}

func (d SimpleDocument) GetVector() []float64 {
	return d.Vector
}

func (d SimpleDocument) Register() {
	gob.Register(d)
}
