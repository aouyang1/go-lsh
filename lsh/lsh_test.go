package lsh

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/aouyang1/go-lsh/configs"
	"github.com/aouyang1/go-lsh/document"
	"github.com/aouyang1/go-lsh/lsherrors"
	"github.com/aouyang1/go-lsh/options"
	"github.com/aouyang1/go-lsh/results"
	"github.com/aouyang1/go-lsh/stats"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

func TestNewLSH(t *testing.T) {
	opt := configs.NewDefaultLSHConfigs()
	_, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLSHSearch(t *testing.T) {
	opt := configs.NewDefaultLSHConfigs()
	lsh, err := New(opt)
	if err != nil {
		t.Fatal(err)
	}

	docs := []document.Document{
		document.NewSimple(0, 0, []float64{0, 0, 5}),
		document.NewSimple(1, 0, []float64{0, 0.1, 3}),
		document.NewSimple(2, 0, []float64{0, 0.1, 2}),
		document.NewSimple(3, 0, []float64{0, 0.1, 1}),
		document.NewSimple(4, 0, []float64{0, -0.1, -4}),
	}
	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	so := options.NewDefaultSearch()
	so.NumToReturn = 3
	so.SignFilter = options.SignFilter_POS

	d := document.Simple{
		Index:  0,
		Vector: []float64{0, 0, 0.1},
	}
	scores, _, err := lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected := []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	if err := lsh.Delete(2); err != nil {
		t.Fatal(err)
	}

	d = document.Simple{Vector: []float64{0, 0, 0.1}}
	scores, _, err = lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 3}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	if err := lsh.Index(document.NewSimple(2, 0, []float64{0, 0.1, 2})); err != nil {
		t.Fatal(err)
	}

	d = document.Simple{Vector: []float64{0, 0, 0.1}}
	scores, _, err = lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.SignFilter = options.SignFilter_NEG
	d = document.Simple{
		Index:  0,
		Vector: []float64{0, 0, 0.1},
	}
	scores, _, err = lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{4}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.SignFilter = options.SignFilter_ANY
	d = document.Simple{Vector: []float64{0, 0, 0.1}}
	scores, _, err = lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 4, 1}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	so.Threshold = 1
	d = document.Simple{Vector: []float64{0, 0, 0.1}}
	scores, _, err = lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

}

/* Needs more though to serializing and deserializing the index
func TestSaveLoadLSH(t *testing.T) {
	cfg := configs.NewDefaultLSHConfigs()
	lsh, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	docs := []document.Document{
		document.NewSimple(0, 0, []float64{0, 0, 5}),
		document.NewSimple(1, 0, []float64{0, 0.1, 3}),
		document.NewSimple(2, 0, []float64{0, 0.1, 2}),
		document.NewSimple(3, 0, []float64{0, 0.1, 1}),
	}
	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	so := options.NewDefaultSearch()
	so.NumToReturn = 3
	so.SignFilter = options.SignFilter_POS

	d := document.Simple{Vector: []float64{0, 0, 0.1}}
	scores, _, err := lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected := []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}

	lshFile := "test.lsh"
	if err := lsh.Save(lshFile, document.Simple{}); err != nil {
		os.Remove(lshFile)
		t.Fatal(err)
	}
	defer os.Remove(lshFile)

	newLsh := new(LSH)
	if err := newLsh.Load(lshFile); err != nil {
		t.Fatal(err)
	}
	newLsh.Cfg.TFunc = configs.NewDefaultTransformFunc
	d = document.Simple{Vector: []float64{0, 0, 0.1}}
	scores, _, err = newLsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = []uint64{0, 1, 2}
	if err := compareUint64s(expected, scores.UIDs()); err != nil {
		t.Fatal(err)
	}
}
*/

func TestIndexSimple(t *testing.T) {
	cfg := configs.NewDefaultLSHConfigs()
	lsh, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		doc         document.Document
		expectedErr error
	}{
		{document.NewSimple(0, 0, []float64{0, 1}), ErrInvalidDocument},
		{document.NewSimple(1, 0, []float64{3, 3, 3}), ErrNoVectorComplexity},
		{document.NewSimple(2, 0, []float64{3, 3, 0}), nil},
		{document.NewSimple(2, 0, []float64{1, 2, 3}), nil},
	}
	for _, td := range testData {
		if err := lsh.Index(td.doc); err != td.expectedErr {
			t.Errorf("expected %v, but got %v for error", td.expectedErr, err)
		}
	}
}

func TestDelete(t *testing.T) {
	cfg := configs.NewDefaultLSHConfigs()
	lsh, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	docs := []document.Document{
		document.NewSimple(0, 0, []float64{0, 1, 3}),
		document.NewSimple(1, 0, []float64{1, 3, 3}),
		document.NewSimple(2, 0, []float64{3, 3, 0}),
		document.NewSimple(3, 0, []float64{1, 2, 3}),
	}

	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	if err := lsh.Delete(2); err != nil {
		t.Fatal(err)
	}

	if err := lsh.Delete(2); err != lsherrors.DocumentNotStored {
		t.Fatalf("expected %v but got %v error", lsherrors.DocumentNotStored, err)
	}
}

func TestSearch(t *testing.T) {
	cfg := configs.NewDefaultLSHConfigs()
	lsh, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	docs := []document.Document{
		document.NewSimple(0, 0, []float64{0, 1, 3}),
		document.NewSimple(1, 0, []float64{1, 3, 3}),
		document.NewSimple(2, 0, []float64{3, 3, 0}),
		document.NewSimple(3, 0, []float64{1, 2, 3}),
	}

	docGroups := []document.Document{
		document.NewSimple(4, 0, []float64{-7, 8, -9}),
		document.NewSimple(5, 0, []float64{-7, 9, -5.5}),
		document.NewSimple(6, 0, []float64{-7, 9, -7}),
		document.NewSimple(7, 0, []float64{-7, 10, -7}),
		document.NewSimple(8, 0, []float64{-5, -3, -2}),
	}

	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range docGroups {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	so := options.NewDefaultSearch()
	d := document.Simple{Vector: []float64{1, 2}}
	if _, _, err := lsh.Search(d, so); err != ErrInvalidDocument {
		t.Fatalf("expected %v, but got %v error", ErrInvalidDocument, err)
	}

	so.NumToReturn = 0
	d = document.Simple{Vector: []float64{1, 2, 3}}
	if _, _, err := lsh.Search(d, so); err != options.ErrInvalidNumToReturn {
		t.Fatalf("expected %v, but got %v error", options.ErrInvalidNumToReturn, err)
	}

	so = options.NewDefaultSearch()
	so.SignFilter = options.SignFilter_POS
	testData := []struct {
		d        document.Document
		sf       options.SignFilter
		expected results.Scores
	}{
		{
			document.Simple{Vector: []float64{0, 1, 3}},
			options.SignFilter_POS,
			results.Scores{
				{UID: 0, Score: 1.00},
				{UID: 3, Score: 0.98},
			},
		},
		{
			document.Simple{Vector: []float64{-7, 9, -7}},
			options.SignFilter_POS,
			results.Scores{
				{UID: 6, Score: 1.00},
				{UID: 7, Score: 0.99},
				{UID: 5, Score: 0.99},
				{UID: 4, Score: 0.99},
			},
		},
	}

	for _, td := range testData {
		so.SignFilter = td.sf
		res, _, err := lsh.Search(td.d, so)
		if err != nil {
			t.Fatal(err)
		}
		if err := compareScores(res, td.expected); err != nil {
			t.Fatalf("%v, res: %v, test data: %v", err, res, td)
		}
	}

}

func TestSearchAcrossTime(t *testing.T) {
	cfg := configs.NewDefaultLSHConfigs()
	cfg.NumHyperplanes = 4
	cfg.RowSize = 60
	lsh, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	docs := []document.Document{
		document.NewSimple(0, 0, []float64{0, 1, 3}),
		document.NewSimple(0, 60, []float64{1, 3, 3}),
		document.NewSimple(0, 120, []float64{3, 3, 0}),
		document.NewSimple(0, 180, []float64{3, 0, 1}),
		document.NewSimple(1, 0, []float64{0, 1, 3}),
		document.NewSimple(1, 60, []float64{1, 3, 3}),
		document.NewSimple(1, 120, []float64{3, 3, 0}),
		document.NewSimple(1, 180, []float64{3, 0, 0}),
	}

	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	so := options.NewDefaultSearch()
	so.MaxLag = -1
	so.Threshold = 1.00
	d := document.Simple{Vector: []float64{1, 3, 3}}
	res, _, err := lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}

	expected := results.Scores{
		{UID: 0, Index: 60, Score: 1.00},
		{UID: 1, Index: 60, Score: 1.00},
		{UID: 1, Index: 180, Score: -1.00},
	}
	if err := compareScores(res, expected); err != nil {
		t.Fatalf("%v, res: %v, expected: %v", err, res, expected)
	}

	// test that we get the right row index``
	so.MaxLag = 0
	d = document.Simple{Index: 60, Vector: []float64{1, 3, 3}}
	res, _, err = lsh.Search(d, so)
	if err != nil {
		t.Fatal(err)
	}
	expected = results.Scores{
		{UID: 0, Index: 60, Score: 1.00},
		{UID: 1, Index: 60, Score: 1.00},
	}
	if err := compareScores(res, expected); err != nil {
		t.Fatalf("%v, res: %v, expected: %v", err, res, expected)
	}
}

func TestLSHError(t *testing.T) {
	numHyperplanes := 8
	numTables := 3
	numIter := 100
	vecLen := 10
	numDocs := 100000
	threshold := 0.85

	cfg := configs.NewDefaultLSHConfigs()
	cfg.NumHyperplanes = numHyperplanes
	cfg.NumTables = numTables
	cfg.VectorLength = vecLen

	lsh, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	vectors := make([][]float64, numDocs)
	for i := 0; i < numDocs; i++ {
		vectors[i] = make([]float64, vecLen)
		for j := 0; j < vecLen; j++ {
			vectors[i][j] = rand.Float64() - 0.5
		}
		floats.Scale(1/floats.Norm(vectors[i], 2), vectors[i])
	}

	start := time.Now()
	for i, f := range vectors {
		if err := lsh.Index(document.NewSimple(uint64(i), 0, f)); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("index time: %v\n", time.Since(start))

	so := options.NewDefaultSearch()
	so.NumToReturn = numDocs
	so.SignFilter = options.SignFilter_POS
	so.Threshold = threshold

	f := make([]float64, vecLen)
	scored := make([]float64, 0, numIter)
	counts := make([]float64, 0, numIter)
	scores := make([]float64, 0, numIter)
	for i := 0; i < numIter; i++ {
		for j := 0; j < vecLen; j++ {
			f[j] = rand.Float64() - 0.5
		}
		floats.Scale(1/floats.Norm(f, 2), f)
		d := document.Simple{Vector: f}
		res, nscored, err := lsh.Search(d, so)
		if err != nil {
			t.Fatal(err)
		}
		scored = append(scored, float64(nscored))
		counts = append(counts, float64(len(res)))
		if len(res) > 0 {
			scores = append(scores, res[len(res)-1].Score)
		}
	}
	nsm, nsstd := stat.MeanStdDev(scored, nil)
	cm, cstd := stat.MeanStdDev(counts, nil)
	sm, sstd := stat.MeanStdDev(scores, nil)
	t.Logf("iterations: %d, num_scored: %d +/-%d, count: %d +/-%d, low_scores: %.3f +/-%.3f\n", numIter, int(nsm), int(nsstd), int(cm), int(cstd), sm, sstd)
}

func TestLSHStats(t *testing.T) {
	cfg := configs.NewDefaultLSHConfigs()
	lsh, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	docs := []document.Document{
		document.NewSimple(0, 0, []float64{0, 0, 5}),
		document.NewSimple(1, 0, []float64{0, 0.1, 3}),
		document.NewSimple(2, 0, []float64{0, 0.1, 2}),
		document.NewSimple(3, 0, []float64{0, 0.1, 1}),
		document.NewSimple(4, 0, []float64{0, -0.1, -4}),
	}
	for _, d := range docs {
		if err := lsh.Index(d); err != nil {
			t.Fatal(err)
		}
	}

	s := lsh.Stats()
	expectedS := &stats.Statistics{
		NumDocs: len(docs),
		FalseNegativeErrors: []stats.FalseNegativeError{
			{Threshold: 0.60, Probability: 0.903},
			{Threshold: 0.65, Probability: 0.804},
			{Threshold: 0.70, Probability: 0.636},
			{Threshold: 0.75, Probability: 0.395},
			{Threshold: 0.80, Probability: 0.149},
			{Threshold: 0.85, Probability: 0.018},
			{Threshold: 0.90, Probability: 0.000},
			{Threshold: 0.95, Probability: 0.000},
		},
	}
	if s.NumDocs != expectedS.NumDocs {
		t.Fatalf("expected %d, but got %d docs", expectedS.NumDocs, s.NumDocs)
	}
	if len(s.FalseNegativeErrors) != len(expectedS.FalseNegativeErrors) {
		t.Fatalf("expected %d, but got %d false negative errors", len(expectedS.FalseNegativeErrors), len(s.FalseNegativeErrors))
	}
	for i, fne := range s.FalseNegativeErrors {
		if math.Abs(fne.Threshold-expectedS.FalseNegativeErrors[i].Threshold) > 0.01 {
			t.Errorf("expected %.02f, but got %.02f threshold", expectedS.FalseNegativeErrors[i].Threshold, fne.Threshold)
		}
		if math.Abs(fne.Probability-expectedS.FalseNegativeErrors[i].Probability) > 0.001 {
			t.Errorf("expected %.03f, but got %.03f probability", expectedS.FalseNegativeErrors[i].Probability, fne.Probability)
		}
	}
}
func compareUint64s(expected, uids []uint64) error {
	if len(uids) != len(expected) {
		return fmt.Errorf("expected %d results, but got %d", len(expected), len(uids))
	}
	for i, uid := range uids {
		if uid != expected[i] {
			return fmt.Errorf("expected %v, but got %v", expected, uids)
		}
	}
	return nil
}

func compareScores(res, expected results.Scores) error {
	if len(res) != len(expected) {
		return fmt.Errorf("expected %d scores, but got %d", len(expected), len(res))
	}
	sort.Sort(res)
	sort.Sort(expected)
	for i, s := range expected {
		if s.UID != res[i].UID {
			return fmt.Errorf("expected uid %d, but got %d", s.UID, res[i].UID)
		}
		if math.Abs(s.Score-res[i].Score) > 0.01 {
			return fmt.Errorf("expected score %.2f, but got %.2f", s.Score, res[i].Score)
		}
	}
	return nil
}

func BenchmarkLSHIndex(b *testing.B) {
	cfg := configs.NewDefaultLSHConfigs()
	cfg.VectorLength = 60
	lsh, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		vec := make([]float64, cfg.VectorLength)
		for j := 0; j < cfg.VectorLength; j++ {
			vec[j] = rand.Float64()
		}

		doc := document.NewSimple(uint64(i), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLSHSearchSingleHyperplane(b *testing.B) {
	cfg := configs.NewDefaultLSHConfigs()
	cfg.VectorLength = 60
	cfg.NumHyperplanes = 1
	lsh, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}

	numDocuments := 10000
	for n := 0; n < numDocuments; n++ {
		vec := make([]float64, cfg.VectorLength)
		for j := 0; j < cfg.VectorLength; j++ {
			vec[j] = rand.Float64()
		}

		doc := document.NewSimple(uint64(n), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}

	query := make([]float64, cfg.VectorLength)
	for j := 0; j < cfg.VectorLength; j++ {
		query[j] = rand.Float64()
	}
	d := document.Simple{Vector: query}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := lsh.Search(d, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLSHSearchPositive(b *testing.B) {
	cfg := configs.NewDefaultLSHConfigs()
	cfg.VectorLength = 60
	lsh, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}

	numDocuments := 10000
	for n := 0; n < numDocuments; n++ {
		vec := make([]float64, cfg.VectorLength)
		for j := 0; j < cfg.VectorLength; j++ {
			vec[j] = rand.Float64()
		}

		doc := document.NewSimple(uint64(n), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}

	query := make([]float64, cfg.VectorLength)
	for j := 0; j < cfg.VectorLength; j++ {
		query[j] = rand.Float64()
	}
	d := document.Simple{Vector: query}
	so := options.NewDefaultSearch()
	so.SignFilter = options.SignFilter_POS

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := lsh.Search(d, so)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLSHSearchRealistic(b *testing.B) {
	cfg := configs.NewDefaultLSHConfigs()
	cfg.VectorLength = 60
	lsh, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}

	waveforms := make(map[string][]float64)
	spike := make([]float64, cfg.VectorLength)
	spike[cfg.VectorLength/2] = 1.0
	waveforms["spike"] = spike

	risingstep := make([]float64, cfg.VectorLength)
	for i := cfg.VectorLength / 2; i < cfg.VectorLength; i++ {
		risingstep[i] = 1.0
	}
	waveforms["risingstep"] = risingstep

	loweringstep := make([]float64, cfg.VectorLength)
	for i := cfg.VectorLength / 2; i < cfg.VectorLength; i++ {
		loweringstep[i] = -1.0
	}
	waveforms["loweringstep"] = loweringstep

	triangle := make([]float64, cfg.VectorLength)
	for i := cfg.VectorLength / 4; i < cfg.VectorLength/2; i++ {
		triangle[i] = 1 * float64(i-cfg.VectorLength/4)
	}
	for i := cfg.VectorLength / 2; i < 3*cfg.VectorLength/4; i++ {
		triangle[i] = -1*float64(i-cfg.VectorLength/2) + 1
	}
	waveforms["triangle"] = triangle

	dip := make([]float64, cfg.VectorLength)
	for i := cfg.VectorLength / 4; i < cfg.VectorLength/2; i++ {
		dip[i] = -1 * float64(i-cfg.VectorLength/4)
	}
	for i := cfg.VectorLength / 2; i < 3*cfg.VectorLength/4; i++ {
		dip[i] = 1*float64(i-cfg.VectorLength/2) - 1
	}
	waveforms["dip"] = dip

	waveNames := []string{"spike", "risingstep", "loweringstep", "triangle", "dip"}

	numDocuments := 100000
	for n := 0; n < numDocuments; n++ {
		vec := make([]float64, cfg.VectorLength)
		copy(vec, waveforms[waveNames[n%len(waveNames)]])
		for j := 0; j < cfg.VectorLength; j++ {
			vec[j] += rand.Float64()
		}
		doc := document.NewSimple(uint64(n), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}

	query := waveforms["risingstep"]
	d := document.Simple{Vector: query}

	so := options.NewDefaultSearch()
	so.SignFilter = options.SignFilter_POS
	so.NumToReturn = 30000
	so.Threshold = 0.65

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, _, err := lsh.Search(d, so)
		if err != nil {
			b.Fatal(err)
		}
		if len(res) != 20000 {
			b.Fatalf("unexpected number of results, %d", len(res))
		}
	}
}

func BenchmarkLSHSearchRealisticSingleHyperplane(b *testing.B) {
	cfg := configs.NewDefaultLSHConfigs()
	cfg.VectorLength = 60
	cfg.NumHyperplanes = 1
	lsh, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}

	waveforms := make(map[string][]float64)
	spike := make([]float64, cfg.VectorLength)
	spike[cfg.VectorLength/2] = 1.0
	waveforms["spike"] = spike

	risingstep := make([]float64, cfg.VectorLength)
	for i := cfg.VectorLength / 2; i < cfg.VectorLength; i++ {
		risingstep[i] = 1.0
	}
	waveforms["risingstep"] = risingstep

	loweringstep := make([]float64, cfg.VectorLength)
	for i := cfg.VectorLength / 2; i < cfg.VectorLength; i++ {
		loweringstep[i] = -1.0
	}
	waveforms["loweringstep"] = loweringstep

	triangle := make([]float64, cfg.VectorLength)
	for i := cfg.VectorLength / 4; i < cfg.VectorLength/2; i++ {
		triangle[i] = 1 * float64(i-cfg.VectorLength/4)
	}
	for i := cfg.VectorLength / 2; i < 3*cfg.VectorLength/4; i++ {
		triangle[i] = -1*float64(i-cfg.VectorLength/2) + 1
	}
	waveforms["triangle"] = triangle

	dip := make([]float64, cfg.VectorLength)
	for i := cfg.VectorLength / 4; i < cfg.VectorLength/2; i++ {
		dip[i] = -1 * float64(i-cfg.VectorLength/4)
	}
	for i := cfg.VectorLength / 2; i < 3*cfg.VectorLength/4; i++ {
		dip[i] = 1*float64(i-cfg.VectorLength/2) - 1
	}
	waveforms["dip"] = dip

	waveNames := []string{"spike", "risingstep", "loweringstep", "triangle", "dip"}

	numDocuments := 100000
	for n := 0; n < numDocuments; n++ {
		vec := make([]float64, cfg.VectorLength)
		copy(vec, waveforms[waveNames[n%len(waveNames)]])
		for j := 0; j < cfg.VectorLength; j++ {
			vec[j] += rand.Float64()
		}

		doc := document.NewSimple(uint64(n), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}

	query := waveforms["risingstep"]
	d := document.Simple{Vector: query}

	so := options.NewDefaultSearch()
	so.SignFilter = options.SignFilter_POS

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := lsh.Search(d, so)
		if err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkLSHDelete(b *testing.B) {
	cfg := configs.NewDefaultLSHConfigs()
	cfg.VectorLength = 60
	lsh, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}

	numDocuments := 10000
	for n := 0; n < numDocuments; n++ {
		vec := make([]float64, cfg.VectorLength)
		for j := 0; j < cfg.VectorLength; j++ {
			vec[j] = rand.Float64()
		}

		doc := document.NewSimple(uint64(n), 0, vec)
		if err := lsh.Index(doc); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := lsh.Delete(uint64(i))
		if err != nil {
			if err == lsherrors.DocumentNotStored {
				continue
			}
			b.Fatal(err)
		}
	}
}
