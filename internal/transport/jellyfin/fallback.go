package jellyfin

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/Esteviredzu/shampoore/internal/logging"
)

const stubPage = `<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>Jellyfin</title></head>
<body><div id="reactRoot">Jellyfin Server</div></body>
</html>
`

func newFallbackHandler(upstream string, logger logging.ContextLogger) http.Handler {
	upstream = strings.TrimSpace(upstream)
	if upstream == "" {
		return http.HandlerFunc(serveStub)
	}
	target, err := url.Parse(upstream)
	if err != nil || target.Host == "" {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "jellyfin upstream is misconfigured", http.StatusBadGateway)
		})
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.FlushInterval = -1
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		host := r.Host
		proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
		director(r)
		r.Host = host
		if proto == "" {
			proto = "http"
			if r.TLS != nil {
				proto = "https"
			}
		}
		r.Header.Set("X-Forwarded-Proto", proto)
		r.Header.Set("X-Forwarded-Host", host)
		if r.Header.Get("X-Forwarded-For") == "" {
			if clientIP, _, splitErr := net.SplitHostPort(r.RemoteAddr); splitErr == nil {
				r.Header.Set("X-Forwarded-For", clientIP)
			}
		}
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if logger != nil {
			logger.Warn("jellyfin: upstream proxy failed: ", err)
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	return proxy
}

func serveStub(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSuffix(r.URL.Path, "/")
	if path == "" {
		path = "/"
	}
	switch {
	case path == "/System/Info/Public":
		writeJSON(w, map[string]any{
			"ServerName":      "Media Server",
			"Version":         "10.9.11",
			"ProductName":     "Jellyfin Server",
			"OperatingSystem": "Linux",
			"Id":              "6d4b1f1e9f3a4f0d8b6c2a5e7f1d3c9b",
		})
	case path == "/System/Endpoint":
		writeJSON(w, map[string]any{"IsLocal": false, "IsInNetwork": true})
	case path == "/SyncPlay/List":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write([]byte("[]"))
	case path == "/" || path == "/web" || path == "/web/index.html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(stubPage))
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		http.Error(w, fmt.Sprint(err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(body)
}
