package options

import "testing"

func TestSearchOptionsValidate(t *testing.T) {
	testData := []struct {
		numToReturn int
		threshold   float64
		signFilter  SignFilter

		expectedErr error
	}{
		{0, 0.65, SignFilter_ANY, ErrInvalidNumToReturn},
		{1, 1.3, SignFilter_ANY, ErrInvalidThreshold},
		{1, 0.65, SignFilter(2), ErrInvalidSignFilter},
		{1, 0.65, SignFilter_ANY, nil},
	}

	for _, td := range testData {
		s := &Search{
			NumToReturn: td.numToReturn,
			Threshold:   td.threshold,
			SignFilter:  td.signFilter,
		}
		if err := s.Validate(); err != td.expectedErr {
			t.Errorf("expected %v, but got %v for error", td.expectedErr, err)
			continue
		}
	}
}
