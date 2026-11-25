//go:build race

package httpstream

var (
	bulkThroughputFloor = 0.0
	bulkTransferBytes   = 16 << 20
	bulkReconnectBytes  = 4 << 20
)
