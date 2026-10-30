package multi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
)

func decodeURL(q url.Values) (json.RawMessage, error) {
	conf := q.Get("conf")
	if conf == "" {
		return nil, errors.New("multi: missing conf")
	}
	raw, err := decodeBase64(conf)
	if err != nil {
		return nil, fmt.Errorf("multi: conf base64: %w", err)
	}
	if !json.Valid(raw) {
		return nil, errors.New("multi: conf is not valid JSON")
	}
	var cfg config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("multi: conf: %w", err)
	}
	if len(cfg.Transports) == 0 {
		return nil, errors.New("multi: conf has no transports")
	}
	return json.RawMessage(raw), nil
}

func encodeURL(raw json.RawMessage, q url.Values) error {
	if len(raw) == 0 {
		return errors.New("multi: empty transport")
	}
	q.Set("conf", base64.RawURLEncoding.EncodeToString(raw))
	return nil
}

func decodeBase64(s string) ([]byte, error) {
	for _, enc := range []*base64.Encoding{
		base64.RawURLEncoding,
		base64.URLEncoding,
		base64.RawStdEncoding,
		base64.StdEncoding,
	} {
		if b, err := enc.DecodeString(s); err == nil {
			return b, nil
		}
	}
	return nil, errors.New("unsupported base64")
}