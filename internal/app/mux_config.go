package app

type MuxConfig struct {
	FlushMs        int
	FlushThreshold int

	UserID     string
	ExitNodeID string
}
