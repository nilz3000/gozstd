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

static size_t ZSTD_compressCCtx_wrapper(uintptr_t ctx, uintptr_t dst, size_t dstCapacity, uintptr_t src, size_t srcSize, int compressionLevel) {
    return ZSTD_compressCCtx((ZSTD_CCtx*)ctx, (void*)dst, dstCapacity, (const void*)src, srcSize, compressionLevel);
}

static size_t ZSTD_min_error_code() {
    return ((size_t)-ZSTD_error_maxCode);
}
*/
import "C"

import (
	"runtime"
	"sync"
	"unsafe"
)

const minErrCode = uint(C.ZSTD_MIN_ERR_CODE)
const noErrCode = uint(C.ZSTD_NO_ERR_CODE)

type Encoder struct {
	cctxPool         sync.Pool
	compressionLevel int
}

func newEncoder(compressionLevel int) *Encoder {
	e := &Encoder{
		cctxPool: sync.Pool{
			New: newCCtx,
		},
		compressionLevel: compressionLevel,
	}
	return e
}

func CompressBound(srcSize int) int {
	lowLimit := 131072 // 128 kB
	var margin int
	if srcSize < lowLimit {
		margin = (lowLimit - srcSize) >> 11
	}
	return srcSize + (srcSize >> 8) + margin
}

func (e *Encoder) Compress(dst, src []byte) []byte {
	srcLen := len(src)
	if srcLen == 0 {
		return dst
	}

	cctx := e.cctxPool.Get().(*cctxWrapper)
	defer e.cctxPool.Put(cctx)

	dstLen := len(dst)
	if cap(dst) > dstLen+1024 {
		// Fast path - try compressing without dst resize.
		result := compressInternalE(cctx, dst[dstLen:cap(dst)], src, e.compressionLevel)
		compressedSize := uint(result)
		if compressedSize <= minErrCode {
			// All OK.
			return dst[:uint(dstLen)+compressedSize]
		}
		if C.ZSTD_getErrorCode(result) != C.ZSTD_error_dstSize_tooSmall {
			// Unexpected error.
			return src
		}
	} else if dst == nil {
		dst = make([]byte, CompressBound(srcLen)+1)
		result := compressInternalE(cctx, dst, src, e.compressionLevel)
		compressedSize := uint(result)
		if compressedSize > minErrCode {
			return src
		}
		return dst[:compressedSize]
	}

	// Slow path - resize dst to fit compressed data.
	compressBound := CompressBound(srcLen) + 1
	if n := dstLen + compressBound - cap(dst) + dstLen; n > 0 {
		// This should be optimized since go 1.11 - see https://golang.org/doc/go1.11#performance-compiler.
		dst = append(dst[:cap(dst)], make([]byte, n)...)
	}

	result := compressInternalE(cctx, dst[dstLen:dstLen+compressBound], src, e.compressionLevel)
	compressedSize := uint(result)
	if compressedSize > minErrCode {
		return src
	}
	return dst[:uint(dstLen)+compressedSize]
}

func compressInternalE(cctx *cctxWrapper, dst, src []byte, compressionLevel int) C.size_t {
	result := C.ZSTD_compressCCtx_wrapper(
		C.uintptr_t(uintptr(unsafe.Pointer(cctx.cctx))),
		C.uintptr_t(uintptr(unsafe.Pointer(&dst[0]))),
		C.size_t(cap(dst)),
		C.uintptr_t(uintptr(unsafe.Pointer(&src[0]))),
		C.size_t(len(src)),
		C.int(compressionLevel))
	// Prevent from GC'ing of dst and src during CGO call above.
	runtime.KeepAlive(dst)
	runtime.KeepAlive(src)
	runtime.KeepAlive(cctx)
	return result
}
