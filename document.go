package lsh

import (
	"encoding/gob"
	"sort"
)

type Document interface {
	GetUID() uint64
	GetFeatures() []float64
	GetLabel(string) (string, bool)
	ListLabels() []string
	Register()
}

type SimpleDocument struct {
	UID      uint64            `json:"uid"`
	Features []float64         `json:"features"`
	Labels   map[string]string `json:"labels"`
}

func NewSimpleDocument(uid uint64, f []float64, labels map[string]string) *SimpleDocument {
	return &SimpleDocument{
		UID:      uid,
		Labels:   labels,
		Features: f,
	}
}

func (d SimpleDocument) GetUID() uint64 {
	return d.UID
}

func (d SimpleDocument) GetFeatures() []float64 {
	return d.Features
}

func (d SimpleDocument) GetLabel(label string) (string, bool) {
	if d.Labels == nil {
		return "", false
	}
	val, exists := d.Labels[label]
	return val, exists
}

func (d SimpleDocument) ListLabels() []string {
	if d.Labels == nil {
		return nil
	}
	labels := make([]string, 0, len(d.Labels))
	for k := range d.Labels {
		labels = append(labels, k)
	}
	sort.Strings(labels)
	return labels
}

func (d SimpleDocument) Register() {
	gob.Register(d)
}
