package lsh

import (
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
	h, err := NewHyperplanes(nh, nf)
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
		t.Error(err)
		return
	}
	if len(lsh.tables) != opt.NumTables {
		t.Errorf("expected %d tables, but got %d", opt.NumTables, len(lsh.tables))
		return
	}
}
