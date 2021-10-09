package lsh

import (
	"encoding/gob"
)

type Document interface {
	GetUID() uint64
	GetFeatures() []float64
	Register()
}

type SimpleDocument struct {
	UID      uint64    `json:"uid"`
	Features []float64 `json:"features"`
}

func NewSimpleDocument(uid uint64, f []float64) *SimpleDocument {
	return &SimpleDocument{
		UID:      uid,
		Features: f,
	}
}

func (d SimpleDocument) GetUID() uint64 {
	return d.UID
}

func (d SimpleDocument) GetFeatures() []float64 {
	return d.Features
}

func (d SimpleDocument) Register() {
	gob.Register(d)
}
