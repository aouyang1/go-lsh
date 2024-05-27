package configs

import "testing"

func TestNewLSHConfigs(t *testing.T) {
	testData := []struct {
		nf int
		nh int
		nt int
		sp int64
		rs int64

		err error
	}{
		{1, 1, 1, 1, 1, nil},
		{3, 5, 2, 60, 7200, nil},
		{0, 0, 0, 0, 0, ErrInvalidNumHyperplanes},
		{3, 65, 2, 0, 0, ErrExceededMaxNumHyperplanes},
		{0, 5, 2, 0, 0, ErrInvalidVectorLength},
		{3, 5, 0, 0, 0, ErrInvalidNumTables},
		{3, 5, 2, 0, 0, ErrInvalidSamplePeriod},
		{3, 5, 2, 60, 0, ErrInvalidRowSize},
	}
	for _, td := range testData {
		opt := &LSHConfigs{td.nh, td.nt, td.nf, td.sp, td.rs, NewDefaultTransformFunc}
		if err := opt.Validate(); err != td.err {
			t.Errorf("expected %v, but got %v", td.err, err)
			continue
		}
	}
}
