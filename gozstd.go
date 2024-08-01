package gozstd

/*
#cgo CFLAGS: -O3

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"
#include "zstd_errors.h"

#include <stdint.h>  // for uintptr_t

#define ZSTD_MIN_ERR_CODE ((size_t) - ZSTD_error_maxCode)

// The following *_wrapper functions allow avoiding memory allocations
// durting calls from Go.
// See https://github.com/golang/go/issues/24450 .

static size_t ZSTD_compressCCtx_wrapper(uintptr_t ctx, uintptr_t dst, size_t dstCapacity, uintptr_t src, size_t srcSize, int compressionLevel) {
    return ZSTD_compressCCtx((ZSTD_CCtx*)ctx, (void*)dst, dstCapacity, (const void*)src, srcSize, compressionLevel);
}

static size_t ZSTD_decompressDCtx_wrapper(uintptr_t ctx, uintptr_t dst, size_t dstCapacity, uintptr_t src, size_t srcSize) {
    return ZSTD_decompressDCtx((ZSTD_DCtx*)ctx, (void*)dst, dstCapacity, (const void*)src, srcSize);
}

static unsigned long long ZSTD_getFrameContentSize_wrapper(uintptr_t src, size_t srcSize) {
    return ZSTD_getFrameContentSize((const void*)src, srcSize);
}

static size_t ZSTD_min_error_code() {
    return ((size_t)-ZSTD_error_maxCode);
}

*/
import "C"

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"unsafe"
)

const minErrCode = uint(C.ZSTD_MIN_ERR_CODE)

// DefaultCompressionLevel is the default compression level.
const DefaultCompressionLevel = 3 // Obtained from ZSTD_CLEVEL_DEFAULT.

// Compress appends compressed src to dst and returns the result.
func Compress(dst, src []byte) []byte {
	return CompressLevel(dst, src, DefaultCompressionLevel)
}

// CompressLevel appends compressed src to dst and returns the result.
//
// The given compressionLevel is used for the compression.
func CompressLevel(dst, src []byte, compressionLevel int) []byte {
	cctx := cctxPool.Get().(*cctxWrapper)
	dst = compress(cctx, dst, src, compressionLevel)
	cctxPool.Put(cctx)

	return dst
}

var cctxPool = &sync.Pool{
	New: newCCtx,
}

func newCCtx() interface{} {
	cctx := C.ZSTD_createCCtx()
	cw := &cctxWrapper{
		cctx: cctx,
	}
	runtime.SetFinalizer(cw, freeCCtx)
	return cw
}

func freeCCtx(cw *cctxWrapper) {
	C.ZSTD_freeCCtx(cw.cctx)
	cw.cctx = nil
}

type cctxWrapper struct {
	cctx *C.ZSTD_CCtx
}

func CompressBound(srcSize int) int {
	lowLimit := 131072 // 128 kB
	var margin int
	if srcSize < lowLimit {
		margin = (lowLimit - srcSize) >> 11
	}
	return srcSize + (srcSize >> 8) + margin
}

func compress(cctx *cctxWrapper, dst, src []byte, compressionLevel int) []byte {
	srcLen := len(src)

	if srcLen == 0 {
		return dst
	}

	dstLen := len(dst)
	if cap(dst) > dstLen+1024 {
		// Fast path - try compressing without dst resize.
		result := compressInternal(cctx, dst[dstLen:cap(dst)], src, compressionLevel, false)
		compressedSize := int(result)
		if compressedSize >= 0 {
			// All OK.
			return dst[:dstLen+compressedSize]
		}
		if C.ZSTD_getErrorCode(result) != C.ZSTD_error_dstSize_tooSmall {
			// Unexpected error.
			panic(fmt.Errorf("BUG: unexpected error during compression with: %s", errStr(result)))
		}
	} else if dst == nil {
		dst = make([]byte, CompressBound(srcLen)+1)
		result := compressInternal(cctx, dst, src, compressionLevel, true)
		compressedSize := uint(result)
		return dst[:compressedSize]
	}

	// Slow path - resize dst to fit compressed data.
	compressBound := CompressBound(srcLen) + 1
	if n := dstLen + compressBound - cap(dst) + dstLen; n > 0 {
		// This should be optimized since go 1.11 - see https://golang.org/doc/go1.11#performance-compiler.
		dst = append(dst[:cap(dst)], make([]byte, n)...)
	}

	result := compressInternal(cctx, dst[dstLen:dstLen+compressBound], src, compressionLevel, true)
	compressedSize := int(result)
	dst = dst[:dstLen+compressedSize]
	return dst
}

func compressInternal(cctx *cctxWrapper, dst, src []byte, compressionLevel int, mustSucceed bool) C.size_t {
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
	if mustSucceed {
		ensureNoError("ZSTD_compressCCtx_wrapper", result)
	}
	return result
}

// Decompress appends decompressed src to dst and returns the result.
func Decompress(dst, src []byte) ([]byte, error) {
	var err error
	dctx := dctxPool.Get().(*dctxWrapper)
	dst, err = decompress(dctx, dst, src)
	dctxPool.Put(dctx)
	return dst, err
}

var dctxPool = &sync.Pool{
	New: newDCtx,
}

func newDCtx() interface{} {
	dctx := C.ZSTD_createDCtx()
	dw := &dctxWrapper{
		dctx: dctx,
	}
	runtime.SetFinalizer(dw, freeDCtx)
	return dw
}

func freeDCtx(dw *dctxWrapper) {
	C.ZSTD_freeDCtx(dw.dctx)
	dw.dctx = nil
}

type dctxWrapper struct {
	dctx *C.ZSTD_DCtx
}

func decompress(dctx *dctxWrapper, dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return dst, nil
	}

	dstLen := len(dst)
	if cap(dst) > dstLen+1024 {
		// Fast path - try decompressing without dst resize.
		result := decompressInternal(dctx, dst[dstLen:cap(dst)], src)
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

	result := decompressInternal(dctx, dst[dstLen:dstLen+decompressBound], src)
	decompressedSize := int(result)
	if decompressedSize >= 0 {
		dst = dst[:dstLen+decompressedSize]
		return dst, nil
	}

	// Error during decompression.
	return dst[:dstLen], fmt.Errorf("decompression error: %s", errStr(result))
}

func decompressInternal(dctx *dctxWrapper, dst, src []byte) C.size_t {
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

func errStr(result C.size_t) string {
	errCode := C.ZSTD_getErrorCode(result)
	errCStr := C.ZSTD_getErrorString(errCode)
	return C.GoString(errCStr)
}

func ensureNoError(funcName string, result C.size_t) {
	if int(result) >= 0 {
		// Fast path - avoid calling C function.
		return
	}
	if uint(result) > minErrCode {
		panic(fmt.Errorf("BUG: unexpected error in %s: %s", funcName, errStr(result)))
	}
}

func streamDecompress(dst, src []byte, dd *DDict) ([]byte, error) {
	sd := getStreamDecompressor(dd)
	sd.dst = dst
	sd.src = src
	_, err := sd.zr.WriteTo(sd)
	dst = sd.dst
	putStreamDecompressor(sd)
	return dst, err
}

type streamDecompressor struct {
	dst       []byte
	src       []byte
	srcOffset int

	zr *Reader
}

type srcReader streamDecompressor

func (sr *srcReader) Read(p []byte) (int, error) {
	sd := (*streamDecompressor)(sr)
	n := copy(p, sd.src[sd.srcOffset:])
	sd.srcOffset += n
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (sd *streamDecompressor) Write(p []byte) (int, error) {
	sd.dst = append(sd.dst, p...)
	return len(p), nil
}

func getStreamDecompressor(dd *DDict) *streamDecompressor {
	v := streamDecompressorPool.Get()
	if v == nil {
		sd := &streamDecompressor{
			zr: NewReader(nil),
		}
		v = sd
	}
	sd := v.(*streamDecompressor)
	sd.zr.Reset((*srcReader)(sd), dd)
	return sd
}

func putStreamDecompressor(sd *streamDecompressor) {
	sd.dst = nil
	sd.src = nil
	sd.srcOffset = 0
	sd.zr.Reset(nil, nil)
	streamDecompressorPool.Put(sd)
}

var streamDecompressorPool sync.Pool
