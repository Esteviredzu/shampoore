//go:build android

package runtime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

var (
	androidEngineMu sync.Mutex
	androidEngine   *tunEngine
)

func StartTUNFD(ctx context.Context, fd int, mtu int) (<-chan error, error) {
	if fd < 0 {
		return nil, fmt.Errorf("tun/android: invalid fd %d", fd)
	}
	if mtu <= 0 {
		mtu = 1500
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	androidEngineMu.Lock()
	if androidEngine != nil {
		androidEngineMu.Unlock()
		return nil, fmt.Errorf("tun/android: engine already running")
	}
	eng, err := newTunEngine(fd, mtu)
	if err != nil {
		androidEngineMu.Unlock()
		return nil, err
	}
	androidEngine = eng
	androidEngineMu.Unlock()

	log.Printf("tun/android: started on fd=%d, mtu=%d", fd, mtu)

	done := make(chan error, 1)
	go func() {
		var runErr error
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

	loop:
		for {
			select {
			case <-ctx.Done():
				runErr = ctx.Err()
				break loop
			case <-ticker.C:
				if !fdAlive(fd) {
					runErr = errors.New("tun/android: descriptor closed")
					break loop
				}
			}
		}

		log.Println("tun/android: stopping")
		eng.stop()

		androidEngineMu.Lock()
		if androidEngine == eng {
			androidEngine = nil
		}
		androidEngineMu.Unlock()

		done <- runErr
		close(done)
	}()

	return done, nil
}
