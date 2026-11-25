package jellyfin

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/transport/httpstream"
)

const (
	sessionHeader = "X-Emby-Token"

	jellyfinAccept = "video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5"

	progressBody = `{"VolumeLevel":100,"IsMuted":false,"IsPaused":false,"RepeatMode":"RepeatNone","ShuffleMode":"Sorted","MaxStreamingBitrate":140000000,"PositionTicks":124500000,"PlaybackStartTimeTicks":17955127284230000,"PlayMethod":"DirectPlay","PlaySessionId":"{{PAYLOAD_B64}}"}`

	deviceIDBytes = 32
)

func newDeviceID() string {
	buf := make([]byte, deviceIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return base64.StdEncoding.EncodeToString([]byte(time.Now().String()))
	}
	return base64.StdEncoding.EncodeToString(buf)
}

func mediaClientHeader(deviceID string) string {
	return `MediaBrowser Client="Jellyfin%20Web", Device="Chrome", DeviceId="` + deviceID + `", Version="12.4.0"`
}

func defaultProfile() httpstream.MimicProfile {
	deviceID := newDeviceID()
	return httpstream.MimicProfile{
		Name:         "jellyfin",
		DownlinkMode: httpstream.DownlinkStream,
		DownlinkSpecs: []httpstream.RequestSpec{{
			Method: http.MethodGet,
			Paths:  []string{"/Videos/{id}/stream.mp4"},
			Query: map[string][]string{
				"Static":        {"true"},
				"mediaSourceId": {"{id}"},
				"deviceId":      {"{rand}"},
				"Tag":           {"{cursor}"},
			},
			Headers: map[string]string{
				"Accept": jellyfinAccept,
				"Range":  "bytes={seq}-",
			},
		}},
		UplinkSpecs: []httpstream.RequestSpec{{
			Method:       http.MethodPost,
			Paths:        []string{"/Sessions/Playing/Progress", "/Sessions/Playing"},
			ContentType:  "application/json",
			BodyTemplate: progressBody,
			PayloadField: "PlaySessionId",
			Headers: map[string]string{
				"Accept":        "*/*",
				"Authorization": mediaClientHeader(deviceID),
			},
		}},
		SessionPlacement: httpstream.SessionPlacement{
			Mode: httpstream.PlaceHeader,
			Name: sessionHeader,
		},
		MaxStreamBytes:    256 << 20,
		MaxStreamDuration: 30 * time.Minute,

		DownlinkReplayBytes: 16 << 20,
		AckInterval:         500 * time.Millisecond,
		MaxUplinkInflight:   2,
		Padding:             httpstream.Padding{MinPad: 64, MaxPad: 512, TargetBuckets: []int{560, 1536, 4096, 16384, 65536}},
		UplinkBatchWindow:   15 * time.Millisecond,
		MaxUplinkBatchBytes: 32 << 10,
		DecoyRequests: []httpstream.DecoySpec{{
			Paths:       []string{"/System/Endpoint", "/SyncPlay/List"},
			IntervalMin: 12 * time.Second,
			IntervalMax: 25 * time.Second,
		}},
	}
}

func profileFor(utls string) httpstream.MimicProfile {
	profile := defaultProfile()
	profile.UTLSProfile = strings.TrimSpace(utls)
	return profile.Normalize()
}
