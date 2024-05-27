package bitmap

import (
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// Bitmap is a go-routine safe wrapping of the roarding bitmap
type Bitmap struct {
	sync.Mutex

	Rb *roaring64.Bitmap
}

func New() *Bitmap {
	return &Bitmap{Rb: roaring64.New()}
}

func (b *Bitmap) Add(uid uint64) {
	b.Lock()
	defer b.Unlock()
	b.Rb.Add(uid)
}

func (b *Bitmap) CheckedRemove(uid uint64) bool {
	b.Lock()
	defer b.Unlock()
	return b.Rb.CheckedRemove(uid)
}

func (b *Bitmap) IsEmpty() bool {
	b.Lock()
	defer b.Unlock()
	return b.Rb.IsEmpty()
}
