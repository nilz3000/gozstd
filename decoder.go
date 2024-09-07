package gozstd

/*
#cgo CFLAGS: -O3

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"
#include "zstd_errors.h"

#include <stdint.h>  // for uintptr_t

#define ZSTD_MIN_ERR_CODE ((size_t) - ZSTD_error_maxCode)
#define ZSTD_NO_ERR_CODE ((size_t) - 0)

// The following *_wrapper functions allow avoiding memory allocations
// durting calls from Go.
// See https://github.com/golang/go/issues/24450 .

static size_t ZSTD_decompressDCtx_wrapper(uintptr_t ctx, uintptr_t dst, size_t dstCapacity, uintptr_t src, size_t srcSize) {
    return ZSTD_decompressDCtx((ZSTD_DCtx*)ctx, (void*)dst, dstCapacity, (const void*)src, srcSize);
}

static unsigned long long ZSTD_getFrameContentSize_wrapper(uintptr_t src, size_t srcSize) {
    return ZSTD_getFrameContentSize((const void*)src, srcSize);
}
*/
import "C"

import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

// Decompress appends decompressed src to dst and returns the result.

type Decoder struct {
	dctxPool               sync.Pool
	streamDecompressorPool sync.Pool
}

func newStreamDecompressor() interface{} {
	sd := &streamDecompressor{
		zr: NewReader(nil),
	}
	return sd
}

func newDecoder() *Decoder {
	d := &Decoder{
		dctxPool: sync.Pool{
			New: newDCtx,
		},
		streamDecompressorPool: sync.Pool{
			New: newStreamDecompressor,
		},
	}
	return d
}

func (d *Decoder) Decompress(dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return dst, nil
	}

	dctx := d.dctxPool.Get().(*dctxWrapper)
	defer d.dctxPool.Put(dctx)

	dstLen := len(dst)
	if cap(dst) > dstLen+1024 {
		// Fast path - try decompressing without dst resize.
		result := decompressInternalD(dctx, dst[dstLen:cap(dst)], src)
		decompressedSize := int(result)
		if decompressedSize >= 0 {
			// All OK.
			return dst[:dstLen+decompressedSize], nil
		}

		if C.ZSTD_getErrorCode(result) != C.ZSTD_error_dstSize_tooSmall {
			// Error during decompression.
			return dst[:dstLen], fmt.Errorf("decompression error: %s", errStr(result))
		}
	}

	// Slow path - resize dst to fit decompressed data.
	decompressBound := int(C.ZSTD_getFrameContentSize_wrapper(
		C.uintptr_t(uintptr(unsafe.Pointer(&src[0]))), C.size_t(len(src))))
	// Prevent from GC'ing of src during CGO call above.
	runtime.KeepAlive(src)
	switch uint64(decompressBound) {
	case uint64(C.ZSTD_CONTENTSIZE_UNKNOWN):
		return streamDecompress(dst, src, nil)
	case uint64(C.ZSTD_CONTENTSIZE_ERROR):
		return dst, fmt.Errorf("cannot decompress invalid src")
	}
	decompressBound++

	if dst == nil {
		dst = make([]byte, decompressBound)
	} else if n := dstLen + decompressBound - cap(dst); n > 0 {
		// This should be optimized since go 1.11 - see https://golang.org/doc/go1.11#performance-compiler.
		dst = append(dst[:cap(dst)], make([]byte, n)...)
	}

	result := decompressInternalD(dctx, dst[dstLen:dstLen+decompressBound], src)
	decompressedSize := int(result)
	if decompressedSize >= 0 {
		dst = dst[:dstLen+decompressedSize]
		return dst, nil
	}

	// Error during decompression.
	return dst[:dstLen], fmt.Errorf("decompression error: %s", errStr(result))

}

func decompressInternalD(dctx *dctxWrapper, dst, src []byte) C.size_t {
	n := C.ZSTD_decompressDCtx_wrapper(
		C.uintptr_t(uintptr(unsafe.Pointer(dctx.dctx))),
		C.uintptr_t(uintptr(unsafe.Pointer(&dst[0]))),
		C.size_t(cap(dst)),
		C.uintptr_t(uintptr(unsafe.Pointer(&src[0]))),
		C.size_t(len(src)))
	// Prevent from GC'ing of dst and src during CGO calls above.
	runtime.KeepAlive(dst)
	runtime.KeepAlive(src)
	runtime.KeepAlive(dctx)
	return n
}

func (d *Decoder) streamDecompress(dst, src []byte) ([]byte, error) {
	sd := d.getStreamDecompressor()
	sd.dst = dst
	sd.src = src
	_, err := sd.zr.WriteTo(sd)
	dst = sd.dst
	d.putStreamDecompressor(sd)
	return dst, err
}

func (d *Decoder) getStreamDecompressor() *streamDecompressor {
	v := d.streamDecompressorPool.Get()
	sd := v.(*streamDecompressor)
	sd.zr.Reset((*srcReader)(sd), nil)
	return sd
}

func (d *Decoder) putStreamDecompressor(sd *streamDecompressor) {
	sd.dst = nil
	sd.src = nil
	sd.srcOffset = 0
	sd.zr.Reset(nil, nil)
	d.streamDecompressorPool.Put(sd)
}
