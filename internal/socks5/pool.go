package socks5

import "sync"

const copyBufSize = 32 * 1024

var copyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, copyBufSize)
		return &b
	},
}

func getCopyBuf(n int) ([]byte, func()) {
	if n < 0 {
		n = 0
	}
	bp := copyBufPool.Get().(*[]byte)
	if cap(*bp) < n {
		*bp = make([]byte, n)
	}
	buf := (*bp)[:n]
	return buf, func() { copyBufPool.Put(bp) }
}
