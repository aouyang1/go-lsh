package forwardindex

import (
	"github.com/aouyang1/go-lsh/configs"
	"github.com/aouyang1/go-lsh/document"
)

type InMemory struct {
	cfg *configs.LSHConfigs

	docs map[uint64]document.Document
}

func NewInMemory(cfg *configs.LSHConfigs) *InMemory {
	return &InMemory{
		cfg:  cfg,
		docs: make(map[uint64]document.Document),
	}
}

func (i *InMemory) Size() int {
	return len(i.docs)
}

func (i *InMemory) Exists(uid uint64) (document.Document, bool) {
	d, exists := i.docs[uid]
	return d, exists
}

func (i *InMemory) Index(d document.Document) {
	// expand current doc of the uid if present
	if currDoc, exists := i.Exists(d.GetUID()); exists {
		dIdx := d.GetIndex() / i.cfg.SamplePeriod
		cdIdx := currDoc.GetIndex() / i.cfg.SamplePeriod
		offset := int(dIdx - cdIdx)

		origVec := d.GetVector()
		cdVec := currDoc.GetVector()
		if offset > 0 {
			for i := 0; i < len(origVec); i++ {
				idx := i + offset
				if idx < len(cdVec) {
					cdVec[idx] = origVec[i]
				} else {
					zeroPad := idx - len(cdVec)
					if zeroPad > 0 {
						zeros := make([]float64, zeroPad)
						cdVec = append(cdVec, zeros...)
					}
					cdVec = append(cdVec, origVec[i])
				}
			}
		} else {
			// not handling docs that are in the past
		}
		d = document.NewSimple(currDoc.GetUID(), currDoc.GetIndex(), cdVec)
	}
	i.docs[d.GetUID()] = d
}

func (i *InMemory) GetVector(uid uint64, idx int64) []float64 {
	doc, exists := i.Exists(uid)
	if !exists || doc == nil {
		return nil
	}
	vec := doc.GetVector()
	dIdx := doc.GetIndex()

	// just does 0 lag
	startOffset := int((idx - dIdx) / i.cfg.SamplePeriod)
	endOffset := startOffset + i.cfg.VectorLength
	if endOffset > len(vec) {
		endOffset = len(vec)
	}

	buffer := make([]float64, i.cfg.VectorLength)
	for i := 0; i < len(buffer); i++ {
		buffer[i] = 0.0
	}
	copy(buffer, vec[startOffset:endOffset])
	return buffer
}

func (i *InMemory) Delete(uid uint64) {
	delete(i.docs, uid)
}
