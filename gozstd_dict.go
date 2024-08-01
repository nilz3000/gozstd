package gozstd

/*
#cgo CFLAGS: -O3

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"
#include "zstd_errors.h"

#include <stdint.h>  // for uintptr_t

// The following *_wrapper functions allow avoiding memory allocations
// durting calls from Go.
// See https://github.com/golang/go/issues/24450 .

static size_t ZSTD_compress_usingCDict_wrapper(uintptr_t ctx, uintptr_t dst, size_t dstCapacity, uintptr_t src, size_t srcSize, uintptr_t cdict) {
    return ZSTD_compress_usingCDict((ZSTD_CCtx*)ctx, (void*)dst, dstCapacity, (const void*)src, srcSize, (const ZSTD_CDict*)cdict);
}

static size_t ZSTD_decompress_usingDDict_wrapper(uintptr_t ctx, uintptr_t dst, size_t dstCapacity, uintptr_t src, size_t srcSize, uintptr_t ddict) {
    return ZSTD_decompress_usingDDict((ZSTD_DCtx*)ctx, (void*)dst, dstCapacity, (const void*)src, srcSize, (const ZSTD_DDict*)ddict);
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

// CompressDict appends compressed src to dst and returns the result.
//
// The given dictionary is used for the compression.
func CompressDict(dst, src []byte, cd *CDict) []byte {
	return compressDictLevel(dst, src, cd)
}

func compressDictLevel(dst, src []byte, cd *CDict) []byte {
	cctxDict := cctxDictPool.Get().(*cctxWrapper)
	dst = compressDict(cctxDict, dst, src, cd)
	cctxDictPool.Put(cctxDict)
	return dst
}

var cctxDictPool = &sync.Pool{
	New: newCCtx,
}

func compressDict(cctxDict *cctxWrapper, dst, src []byte, cd *CDict) []byte {
	if len(src) == 0 {
		return dst
	}

	dstLen := len(dst)
	if cap(dst) > dstLen {
		// Fast path - try compressing without dst resize.
		result := compressInternalDict(cctxDict, dst[dstLen:cap(dst)], src, cd, false)
		compressedSize := int(result)
		if compressedSize >= 0 {
			// All OK.
			return dst[:dstLen+compressedSize]
		}
		if C.ZSTD_getErrorCode(result) != C.ZSTD_error_dstSize_tooSmall {
			// Unexpected error.
			panic(fmt.Errorf("BUG: unexpected error during compression with cd=%p: %s", cd, errStr(result)))
		}
	}

	// Slow path - resize dst to fit compressed data.
	compressBound := int(C.ZSTD_compressBound(C.size_t(len(src)))) + 1
	if n := dstLen + compressBound - cap(dst) + dstLen; n > 0 {
		// This should be optimized since go 1.11 - see https://golang.org/doc/go1.11#performance-compiler.
		dst = append(dst[:cap(dst)], make([]byte, n)...)
	}

	result := compressInternalDict(cctxDict, dst[dstLen:dstLen+compressBound], src, cd, true)
	compressedSize := int(result)
	dst = dst[:dstLen+compressedSize]
	if cap(dst)-len(dst) > 4096 {
		// Re-allocate dst in order to remove superflouos capacity and reduce memory usage.
		dst = append([]byte{}, dst...)
	}
	return dst
}

func compressInternalDict(cctxDict *cctxWrapper, dst, src []byte, cd *CDict, mustSucceed bool) C.size_t {
	result := C.ZSTD_compress_usingCDict_wrapper(
		C.uintptr_t(uintptr(unsafe.Pointer(cctxDict.cctx))),
		C.uintptr_t(uintptr(unsafe.Pointer(&dst[0]))),
		C.size_t(cap(dst)),
		C.uintptr_t(uintptr(unsafe.Pointer(&src[0]))),
		C.size_t(len(src)),
		C.uintptr_t(uintptr(unsafe.Pointer(cd.p))))
	// Prevent from GC'ing of dst and src during CGO call above.
	runtime.KeepAlive(dst)
	runtime.KeepAlive(src)
	if mustSucceed {
		ensureNoError("ZSTD_compress_usingCDict_wrapper", result)
	}
	return result
}

// DecompressDict appends decompressed src to dst and returns the result.
//
// The given dictionary dd is used for the decompression.
func DecompressDict(dst, src []byte, dd *DDict) ([]byte, error) {
	var err error
	dctxDict := dctxDictPool.Get().(*dctxWrapper)
	dst, err = decompressDict(dctxDict, dst, src, dd)
	dctxDictPool.Put(dctxDict)
	return dst, err
}

var dctxDictPool = &sync.Pool{
	New: newDCtx,
}

func decompressDict(dctxDict *dctxWrapper, dst, src []byte, dd *DDict) ([]byte, error) {
	if len(src) == 0 {
		return dst, nil
	}

	dstLen := len(dst)
	if cap(dst) > dstLen {
		// Fast path - try decompressing without dst resize.
		result := decompressInternalDict(dctxDict, dst[dstLen:cap(dst)], src, dd)
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
		return streamDecompress(dst, src, dd)
	case uint64(C.ZSTD_CONTENTSIZE_ERROR):
		return dst, fmt.Errorf("cannot decompress invalid src")
	}
	decompressBound++

	if n := dstLen + decompressBound - cap(dst); n > 0 {
		// This should be optimized since go 1.11 - see https://golang.org/doc/go1.11#performance-compiler.
		dst = append(dst[:cap(dst)], make([]byte, n)...)
	}

	result := decompressInternalDict(dctxDict, dst[dstLen:dstLen+decompressBound], src, dd)
	decompressedSize := int(result)
	if decompressedSize >= 0 {
		dst = dst[:dstLen+decompressedSize]
		if cap(dst)-len(dst) > 4096 {
			// Re-allocate dst in order to remove superflouos capacity and reduce memory usage.
			dst = append([]byte{}, dst...)
		}
		return dst, nil
	}

	// Error during decompression.
	return dst[:dstLen], fmt.Errorf("decompression error: %s", errStr(result))
}

func decompressInternalDict(dctxDict *dctxWrapper, dst, src []byte, dd *DDict) C.size_t {
	n := C.ZSTD_decompress_usingDDict_wrapper(
		C.uintptr_t(uintptr(unsafe.Pointer(dctxDict.dctx))),
		C.uintptr_t(uintptr(unsafe.Pointer(&dst[0]))),
		C.size_t(cap(dst)),
		C.uintptr_t(uintptr(unsafe.Pointer(&src[0]))),
		C.size_t(len(src)),
		C.uintptr_t(uintptr(unsafe.Pointer(dd.p))))
	// Prevent from GC'ing of dst and src during CGO calls above.
	runtime.KeepAlive(dst)
	runtime.KeepAlive(src)
	return n
}
