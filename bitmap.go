package lsh

import (
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// Bitmap is a go-routine safe wrapping of the roarding bitmap
type Bitmap struct {
	mu sync.Mutex
	Rb *roaring64.Bitmap
}

func newBitmap() *Bitmap {
	return &Bitmap{Rb: roaring64.New()}
}

func (b *Bitmap) CheckedAdd(uid uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Rb.CheckedAdd(uid)
}

func (b *Bitmap) CheckedRemove(uid uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Rb.CheckedRemove(uid)
}

func (b *Bitmap) IsEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Rb.IsEmpty()
}
