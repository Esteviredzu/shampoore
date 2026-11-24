package httpstream

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	mrand "math/rand/v2"

	"github.com/Esteviredzu/shampoore/internal/net/utlsdial"
)

type DownlinkMode string

const (
	DownlinkStream DownlinkMode = "stream"

	DownlinkChunkedPoll DownlinkMode = "chunked_poll"
)

type PlacementMode string

const (
	PlaceCookie PlacementMode = "cookie"
	PlaceBearer PlacementMode = "bearer"
	PlaceHeader PlacementMode = "header"
	PlaceQuery  PlacementMode = "query"
)

const (
	defaultDownlinkPath = "/media/{id}"
	defaultUplinkPath   = "/api/progress"
	defaultCookieName   = "sid"
	defaultHeaderName   = "X-Session"
	defaultQueryName    = "sid"
	defaultAckHeader    = "X-Request-Id"
	cursorQueryParam    = "cursor"
	cursorHeaderName    = "X-HS-Cursor"
	payloadPlaceholder  = "{{PAYLOAD_B64}}"
)

type RequestSpec struct {
	Method       string              `json:"method,omitempty"`
	Paths        []string            `json:"paths,omitempty"`
	Query        map[string][]string `json:"query,omitempty"`
	Headers      map[string]string   `json:"headers,omitempty"`
	ContentType  string              `json:"content_type,omitempty"`
	BodyTemplate string              `json:"body_template,omitempty"`
	PayloadField string              `json:"payload_field,omitempty"`
}

type SessionPlacement struct {
	Mode PlacementMode `json:"mode"`
	Name string        `json:"name,omitempty"`
}

type Padding struct {
	MinPad        int   `json:"min_pad,omitempty"`
	MaxPad        int   `json:"max_pad,omitempty"`
	TargetBuckets []int `json:"target_buckets,omitempty"`
}

type DecoySpec struct {
	Paths       []string          `json:"paths,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	IntervalMin time.Duration     `json:"interval_min,omitempty"`
	IntervalMax time.Duration     `json:"interval_max,omitempty"`
}

type MimicProfile struct {
	Name string `json:"name,omitempty"`

	DownlinkSpecs []RequestSpec `json:"downlink_specs,omitempty"`
	UplinkSpecs   []RequestSpec `json:"uplink_specs,omitempty"`

	SessionPlacement SessionPlacement `json:"session_placement,omitempty"`

	DownlinkMode      DownlinkMode `json:"downlink_mode,omitempty"`
	ImmediateOnUplink bool         `json:"immediate_on_uplink,omitempty"`

	MaxStreamDuration time.Duration `json:"max_stream_duration,omitempty"`
	MaxStreamBytes    int           `json:"max_stream_bytes,omitempty"`

	TargetChunkMin int           `json:"target_chunk_min,omitempty"`
	TargetChunkMax int           `json:"target_chunk_max,omitempty"`
	IdlePauseMin   time.Duration `json:"idle_pause_min,omitempty"`
	IdlePauseMax   time.Duration `json:"idle_pause_max,omitempty"`
	PollHold       time.Duration `json:"poll_hold,omitempty"`

	Padding Padding `json:"padding,omitempty"`

	UplinkBatchWindow   time.Duration `json:"uplink_batch_window,omitempty"`
	MaxUplinkBatchBytes int           `json:"max_uplink_batch_bytes,omitempty"`

	DecoyRequests []DecoySpec `json:"decoy_requests,omitempty"`

	UTLSProfile string `json:"utls_profile,omitempty"`
	AckHeader   string `json:"ack_header,omitempty"`
}

func (p MimicProfile) Normalize() MimicProfile {
	out := p
	out.DownlinkSpecs = append([]RequestSpec(nil), p.DownlinkSpecs...)
	out.UplinkSpecs = append([]RequestSpec(nil), p.UplinkSpecs...)
	if out.DownlinkMode == "" {
		out.DownlinkMode = DownlinkStream
	}
	if out.UTLSProfile == "" {
		out.UTLSProfile = string(utlsdial.DefaultProfile)
	} else {
		out.UTLSProfile = string(utlsdial.NormalizeProfile(utlsdial.Profile(out.UTLSProfile)))
	}
	out.AckHeader = strings.TrimSpace(out.AckHeader)
	if out.AckHeader == "" {
		out.AckHeader = defaultAckHeader
	}
	if out.MaxStreamDuration <= 0 {
		out.MaxStreamDuration = 10 * time.Minute
	}
	if out.MaxStreamBytes <= 0 {
		out.MaxStreamBytes = 32 << 20
	}
	if out.TargetChunkMin <= 0 {
		out.TargetChunkMin = 16 << 10
	}
	if out.TargetChunkMax <= 0 {
		out.TargetChunkMax = 64 << 10
	}
	if out.TargetChunkMax < out.TargetChunkMin {
		out.TargetChunkMax = out.TargetChunkMin
	}
	if out.IdlePauseMin <= 0 {
		out.IdlePauseMin = 100 * time.Millisecond
	}
	if out.IdlePauseMax <= 0 {
		out.IdlePauseMax = 500 * time.Millisecond
	}
	if out.IdlePauseMax < out.IdlePauseMin {
		out.IdlePauseMax = out.IdlePauseMin
	}
	if out.PollHold <= 0 {
		out.PollHold = time.Second
	}
	if out.UplinkBatchWindow <= 0 {
		out.UplinkBatchWindow = 20 * time.Millisecond
	}
	if out.MaxUplinkBatchBytes <= 0 {
		out.MaxUplinkBatchBytes = 32 << 10
	}
	if out.Padding.MinPad < 0 {
		out.Padding.MinPad = 0
	}
	if out.Padding.MaxPad < out.Padding.MinPad {
		out.Padding.MaxPad = out.Padding.MinPad
	}
	if len(out.Padding.TargetBuckets) > 0 {
		buckets := append([]int(nil), out.Padding.TargetBuckets...)
		sort.Ints(buckets)
		uniq := buckets[:0]
		for _, b := range buckets {
			if b > 0 && (len(uniq) == 0 || uniq[len(uniq)-1] != b) {
				uniq = append(uniq, b)
			}
		}
		out.Padding.TargetBuckets = uniq
	}

	if len(out.DownlinkSpecs) == 0 {
		out.DownlinkSpecs = []RequestSpec{{
			Method:  http.MethodGet,
			Paths:   []string{defaultDownlinkPath},
			Headers: map[string]string{"Accept": "application/octet-stream"},
		}}
	}
	if len(out.UplinkSpecs) == 0 {
		out.UplinkSpecs = []RequestSpec{{
			Method:      http.MethodPost,
			Paths:       []string{defaultUplinkPath},
			ContentType: "application/octet-stream",
			Headers:     map[string]string{"Accept": "*/*"},
		}}
	}
	for i := range out.DownlinkSpecs {
		if strings.TrimSpace(out.DownlinkSpecs[i].Method) == "" {
			out.DownlinkSpecs[i].Method = http.MethodGet
		}
		out.DownlinkSpecs[i].Method = strings.ToUpper(strings.TrimSpace(out.DownlinkSpecs[i].Method))
	}
	for i := range out.UplinkSpecs {
		if strings.TrimSpace(out.UplinkSpecs[i].Method) == "" {
			out.UplinkSpecs[i].Method = http.MethodPost
		}
		out.UplinkSpecs[i].Method = strings.ToUpper(strings.TrimSpace(out.UplinkSpecs[i].Method))
	}

	if out.SessionPlacement.Mode == "" {
		out.SessionPlacement.Mode = PlaceCookie
	}
	out.SessionPlacement.Name = strings.TrimSpace(out.SessionPlacement.Name)
	switch out.SessionPlacement.Mode {
	case PlaceCookie:
		if out.SessionPlacement.Name == "" {
			out.SessionPlacement.Name = defaultCookieName
		}
	case PlaceHeader:
		if out.SessionPlacement.Name == "" {
			out.SessionPlacement.Name = defaultHeaderName
		}
	case PlaceQuery:
		if out.SessionPlacement.Name == "" {
			out.SessionPlacement.Name = defaultQueryName
		}
	case PlaceBearer:
		out.SessionPlacement.Name = ""
	}
	return out
}

func (p MimicProfile) Validate() error {
	switch p.DownlinkMode {
	case DownlinkStream, DownlinkChunkedPoll:
	default:
		return fmt.Errorf("httpstream: unsupported downlink mode %q", p.DownlinkMode)
	}
	switch p.SessionPlacement.Mode {
	case PlaceCookie, PlaceHeader, PlaceQuery:
		if strings.TrimSpace(p.SessionPlacement.Name) == "" {
			return fmt.Errorf("httpstream: session placement %q requires a name", p.SessionPlacement.Mode)
		}
		if p.SessionPlacement.Mode == PlaceQuery && p.SessionPlacement.Name == cursorQueryParam {
			return fmt.Errorf("httpstream: session placement query name %q conflicts with cursor", cursorQueryParam)
		}
	case PlaceBearer:
	default:
		return fmt.Errorf("httpstream: unsupported session placement %q", p.SessionPlacement.Mode)
	}
	if len(p.DownlinkSpecs) == 0 {
		return errors.New("httpstream: at least one downlink spec is required")
	}
	if len(p.UplinkSpecs) == 0 {
		return errors.New("httpstream: at least one uplink spec is required")
	}
	for i, spec := range p.DownlinkSpecs {
		if err := spec.validate(http.MethodGet); err != nil {
			return fmt.Errorf("httpstream: downlink spec %d: %w", i, err)
		}
	}
	for i, spec := range p.UplinkSpecs {
		if err := spec.validateUplink(); err != nil {
			return fmt.Errorf("httpstream: uplink spec %d: %w", i, err)
		}
	}
	if p.TargetChunkMin <= 0 || p.TargetChunkMax < p.TargetChunkMin {
		return fmt.Errorf("httpstream: invalid chunk bounds (%d..%d)", p.TargetChunkMin, p.TargetChunkMax)
	}
	if p.IdlePauseMin < 0 || p.IdlePauseMax < p.IdlePauseMin {
		return fmt.Errorf("httpstream: invalid idle pause bounds (%s..%s)", p.IdlePauseMin, p.IdlePauseMax)
	}
	if p.Padding.MinPad < 0 || p.Padding.MaxPad < p.Padding.MinPad {
		return fmt.Errorf("httpstream: invalid padding bounds (%d..%d)", p.Padding.MinPad, p.Padding.MaxPad)
	}
	for _, bucket := range p.Padding.TargetBuckets {
		if bucket <= 0 {
			return fmt.Errorf("httpstream: invalid padding bucket %d", bucket)
		}
	}
	if p.MaxStreamBytes <= 0 || p.MaxStreamDuration <= 0 {
		return errors.New("httpstream: stream limits must be positive")
	}
	if p.MaxUplinkBatchBytes <= frameHeaderSize {
		return fmt.Errorf("httpstream: max uplink batch bytes %d is too small", p.MaxUplinkBatchBytes)
	}
	if p.UplinkBatchWindow < 0 {
		return errors.New("httpstream: uplink batch window must not be negative")
	}
	return nil
}

func (s RequestSpec) validateUplink() error {
	if err := s.validate(""); err != nil {
		return err
	}
	switch strings.ToUpper(strings.TrimSpace(s.Method)) {
	case http.MethodPost, http.MethodPut:
		return nil
	default:
		return fmt.Errorf("method %q is not allowed for uplink requests", s.Method)
	}
}

func (s RequestSpec) validate(requiredMethod string) error {
	if strings.TrimSpace(s.Method) == "" {
		return errors.New("method is required")
	}
	if requiredMethod != "" && !strings.EqualFold(s.Method, requiredMethod) {
		return fmt.Errorf("method %q is not allowed", s.Method)
	}
	if len(s.Paths) == 0 {
		return errors.New("at least one path is required")
	}
	for _, path := range s.Paths {
		if strings.TrimSpace(path) == "" {
			return errors.New("path must not be blank")
		}
	}
	if s.BodyTemplate != "" && strings.TrimSpace(s.ContentType) == "" {
		return errors.New("content_type is required for wrapped bodies")
	}
	return nil
}

type PlaceholderValues struct {
	ID     string
	Seq    uint64
	Rand   string
	Cursor uint64
}

func ExpandTemplate(tmpl string, v PlaceholderValues) string {
	if tmpl == "" {
		return tmpl
	}
	if v.ID == "" {
		v.ID = randomHex(6)
	}
	if v.Rand == "" {
		v.Rand = randomHex(8)
	}
	replacer := strings.NewReplacer(
		"{id}", url.PathEscape(v.ID),
		"{seq}", strconv.FormatUint(v.Seq, 10),
		"{rand}", v.Rand,
		"{cursor}", strconv.FormatUint(v.Cursor, 10),
	)
	return replacer.Replace(tmpl)
}

func (p Padding) PadLen(size int) int {
	if len(p.TargetBuckets) > 0 {
		for _, bucket := range p.TargetBuckets {
			if bucket >= size {
				return bucket - size
			}
		}
	}
	if p.MaxPad <= 0 {
		return 0
	}
	if p.MaxPad <= p.MinPad {
		return p.MinPad
	}
	return p.MinPad + mrand.IntN(p.MaxPad-p.MinPad+1)
}

func (spec RequestSpec) WrapUplinkBody(frame []byte) (string, []byte, error) {
	contentType := strings.TrimSpace(spec.ContentType)
	if spec.BodyTemplate == "" {
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		return contentType, frame, nil
	}
	form := strings.Contains(strings.ToLower(contentType), "x-www-form-urlencoded")
	if contentType == "" {
		form = !strings.HasPrefix(strings.TrimSpace(spec.BodyTemplate), "{")
	}
	encoded := base64.StdEncoding.EncodeToString(frame)
	if form {
		encoded = url.QueryEscape(base64.RawURLEncoding.EncodeToString(frame))
	}
	body := strings.ReplaceAll(spec.BodyTemplate, payloadPlaceholder, encoded)
	if contentType == "" {
		if form {
			contentType = "application/x-www-form-urlencoded"
		} else {
			contentType = "application/json"
		}
	}
	return contentType, []byte(body), nil
}

func (p MimicProfile) UnwrapUplinkBody(contentType string, body []byte) ([]byte, error) {
	if len(body) == 0 {
		return nil, errors.New("httpstream: empty uplink body")
	}
	if _, err := decodeFrame(body, p.maxWireFrame()); err == nil {
		return body, nil
	}

	fields := p.payloadFields()
	mediaType := strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	switch {
	case strings.Contains(mediaType, "json"), strings.HasPrefix(strings.TrimSpace(string(body)), "{"):
		return unwrapJSON(body, fields)
	case strings.Contains(mediaType, "x-www-form-urlencoded"):
		return unwrapForm(body, fields)
	}
	return nil, errors.New("httpstream: unrecognised uplink body")
}

func (p MimicProfile) payloadFields() []string {
	seen := map[string]struct{}{}
	var fields []string
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		fields = append(fields, name)
	}
	add("data")
	for _, spec := range p.UplinkSpecs {
		add(spec.PayloadField)
	}
	return fields
}

func unwrapJSON(body []byte, fields []string) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, fmt.Errorf("httpstream: parse json body: %w", err)
	}
	for _, field := range fields {
		value, ok := obj[field]
		if !ok {
			continue
		}
		var s string
		if err := json.Unmarshal(value, &s); err != nil {
			continue
		}
		if raw, ok := decodeBase64Frame(s); ok {
			return raw, nil
		}
	}
	for _, value := range obj {
		var s string
		if err := json.Unmarshal(value, &s); err != nil {
			continue
		}
		if raw, ok := decodeBase64Frame(s); ok {
			return raw, nil
		}
	}
	return nil, errors.New("httpstream: json body does not contain a frame")
}

func unwrapForm(body []byte, fields []string) ([]byte, error) {
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, fmt.Errorf("httpstream: parse form body: %w", err)
	}
	for _, field := range fields {
		for _, value := range values[field] {
			if raw, ok := decodeBase64Frame(value); ok {
				return raw, nil
			}
		}
	}
	for _, list := range values {
		for _, value := range list {
			if raw, ok := decodeBase64Frame(value); ok {
				return raw, nil
			}
		}
	}
	return nil, errors.New("httpstream: form body does not contain a frame")
}

func decodeBase64Frame(value string) ([]byte, bool) {
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding,
	} {
		raw, err := enc.DecodeString(value)
		if err != nil {
			continue
		}
		if _, err := decodeFrame(raw, 1<<30); err == nil {
			return raw, true
		}
	}
	return nil, false
}

func (p MimicProfile) maxWireFrame() int {
	maxSize := frameHeaderSize + p.MaxUplinkBatchBytes + 4096
	if p.Padding.MaxPad > 0 {
		maxSize += p.Padding.MaxPad
	}
	if len(p.Padding.TargetBuckets) > 0 {
		maxSize += p.Padding.TargetBuckets[len(p.Padding.TargetBuckets)-1]
	}
	if maxSize < 64<<10 {
		maxSize = 64 << 10
	}
	return maxSize
}

func (p MimicProfile) nextSpec(specs []RequestSpec) (RequestSpec, string) {
	if len(specs) == 0 {
		return RequestSpec{}, ""
	}
	spec := specs[mrand.IntN(len(specs))]
	if len(spec.Paths) == 0 {
		return spec, ""
	}
	return spec, spec.Paths[mrand.IntN(len(spec.Paths))]
}
