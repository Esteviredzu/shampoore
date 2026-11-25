//go:build !race

package httpstream

var (
	bulkThroughputFloor = 4.0
	bulkTransferBytes   = 50 << 20
	bulkReconnectBytes  = 8 << 20
)
