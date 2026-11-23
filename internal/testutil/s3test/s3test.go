package s3test

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type storedObject struct {
	data     []byte
	modified time.Time
}

type Server struct {
	*httptest.Server

	mu      sync.Mutex
	buckets map[string]map[string]storedObject
}

func New(t testing.TB) *Server {
	t.Helper()
	s := &Server{buckets: make(map[string]map[string]storedObject)}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.Close)
	return s
}

func (s *Server) Endpoint() string {
	u, err := url.Parse(s.URL)
	if err != nil {
		return ""
	}
	return u.Host
}

func (s *Server) Object(bucket, key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	objects, ok := s.buckets[bucket]
	if !ok {
		return nil, false
	}
	obj, ok := objects[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), obj.data...), true
}

func (s *Server) Count(bucket, prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for key := range s.buckets[bucket] {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	key = strings.TrimPrefix(key, "/")
	if bucket == "" {
		http.NotFound(w, r)
		return
	}

	if key == "" {
		s.handleBucket(w, r, bucket)
		return
	}
	s.handleObject(w, r, bucket, key)
}

func (s *Server) handleBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	switch r.Method {
	case http.MethodHead:
		if s.bucketExists(bucket) {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	case http.MethodPut:
		s.makeBucket(bucket)
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		s.mu.Lock()
		delete(s.buckets, bucket)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		if _, ok := query["location"]; ok {
			writeXML(w, http.StatusOK, locationResult{Value: "us-east-1"})
			return
		}
		if !s.bucketExists(bucket) {
			writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "bucket does not exist")
			return
		}
		s.listObjects(w, r, bucket)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch r.Method {
	case http.MethodPut:
		if _, ok := r.URL.Query()["uploads"]; ok {
			writeS3Error(w, http.StatusNotImplemented, "NotImplemented", "multipart uploads are not supported")
			return
		}
		body, err := readBody(r)
		if err != nil {
			writeS3Error(w, http.StatusBadRequest, "IncompleteBody", err.Error())
			return
		}
		s.mu.Lock()
		if s.buckets[bucket] == nil {
			s.buckets[bucket] = make(map[string]storedObject)
		}
		s.buckets[bucket][key] = storedObject{data: body, modified: time.Now().UTC()}
		s.mu.Unlock()
		w.Header().Set("ETag", etagOf(body))
		w.WriteHeader(http.StatusOK)
	case http.MethodGet, http.MethodHead:
		s.mu.Lock()
		obj, ok := s.buckets[bucket][key]
		s.mu.Unlock()
		if !ok {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey", "key does not exist")
			return
		}
		writeObject(w, r, obj, r.Method == http.MethodHead)
	case http.MethodDelete:
		s.mu.Lock()
		if objects, ok := s.buckets[bucket]; ok {
			delete(objects, key)
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) bucketExists(bucket string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.buckets[bucket]
	return ok
}

func (s *Server) makeBucket(bucket string) {
	s.mu.Lock()
	if s.buckets[bucket] == nil {
		s.buckets[bucket] = make(map[string]storedObject)
	}
	s.mu.Unlock()
}

type listEntry struct {
	key      string
	size     int64
	modified time.Time
	etag     string
}

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	startAfter := query.Get("start-after")
	if startAfter == "" {
		startAfter = query.Get("marker")
	}
	escapeKeys := strings.EqualFold(query.Get("encoding-type"), "url")

	s.mu.Lock()
	entries := make([]listEntry, 0, len(s.buckets[bucket]))
	for key, obj := range s.buckets[bucket] {
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		if startAfter != "" && key <= startAfter {
			continue
		}
		sum := md5.Sum(obj.data)
		entries = append(entries, listEntry{
			key:      key,
			size:     int64(len(obj.data)),
			modified: obj.modified,
			etag:     hex.EncodeToString(sum[:]),
		})
	}
	s.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })

	var out strings.Builder
	out.WriteString(xml.Header)
	out.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	out.WriteString("<Name>" + xmlEscape(bucket) + "</Name>")
	out.WriteString("<Prefix>" + xmlEscape(prefix) + "</Prefix>")
	if query.Get("list-type") != "2" {
		out.WriteString("<Marker>" + xmlEscape(query.Get("marker")) + "</Marker>")
	} else if escapeKeys {
		out.WriteString("<EncodingType>url</EncodingType>")
	}
	out.WriteString("<MaxKeys>1000</MaxKeys>")
	out.WriteString("<IsTruncated>false</IsTruncated>")
	for _, entry := range entries {
		key := entry.key
		if escapeKeys {
			key = url.QueryEscape(key)
		}
		out.WriteString("<Contents>")
		out.WriteString("<Key>" + xmlEscape(key) + "</Key>")
		out.WriteString("<LastModified>" + entry.modified.Format(time.RFC3339Nano) + "</LastModified>")
		out.WriteString("<ETag>&quot;" + entry.etag + "&quot;</ETag>")
		out.WriteString("<Size>" + strconv.FormatInt(entry.size, 10) + "</Size>")
		out.WriteString("<StorageClass>STANDARD</StorageClass>")
		out.WriteString("</Contents>")
	}
	out.WriteString("</ListBucketResult>")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, out.String())
}
func writeObject(w http.ResponseWriter, r *http.Request, obj storedObject, head bool) {
	lastModified := obj.modified.UTC().Format(http.TimeFormat)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", etagOf(obj.data))
	w.Header().Set("Last-Modified", lastModified)
	w.Header().Set("Accept-Ranges", "bytes")

	body := obj.data
	status := http.StatusOK
	if raw := r.Header.Get("Range"); raw != "" {
		start, end, ok := parseRange(raw, int64(len(body)))
		if !ok {
			writeS3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", raw)
			return
		}
		body = body[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
		status = http.StatusPartialContent
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if !head {
		_, _ = w.Write(body)
	}
}

func parseRange(raw string, size int64) (int64, int64, bool) {
	raw = strings.TrimSpace(strings.TrimPrefix(raw, "bytes="))
	startPart, endPart, ok := strings.Cut(raw, "-")
	if !ok {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(strings.TrimSpace(startPart), 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	end := size - 1
	if endPart != "" {
		parsed, err := strconv.ParseInt(strings.TrimSpace(endPart), 10, 64)
		if err != nil {
			return 0, 0, false
		}
		if parsed < end {
			end = parsed
		}
	}
	if end < start {
		return 0, 0, false
	}
	return start, end, true
}

type locationResult struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Value   string   `xml:",chardata"`
}

func writeXML(w http.ResponseWriter, status int, value any) {
	data, err := xml.Marshal(value)
	if err != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write(append([]byte(xml.Header), data...))
}

func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xml.Header+`<Error><Code>`+xmlEscape(code)+
		`</Code><Message>`+xmlEscape(message)+`</Message></Error>`)
}

func readBody(r *http.Request) ([]byte, error) {
	chunked := strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") ||
		strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-")
	if !chunked {
		return io.ReadAll(r.Body)
	}
	return readAWSChunked(bufio.NewReader(r.Body))
}

func readAWSChunked(r *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		sizePart := line
		if idx := strings.IndexByte(line, ';'); idx >= 0 {
			sizePart = line[:idx]
		}
		size, err := strconv.ParseInt(strings.TrimSpace(sizePart), 16, 64)
		if err != nil {
			return nil, fmt.Errorf("aws-chunked: bad size %q: %w", sizePart, err)
		}
		if size == 0 {
			for {
				trailer, err := r.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						return out, nil
					}
					return nil, err
				}
				if strings.TrimRight(trailer, "\r\n") == "" {
					return out, nil
				}
			}
		}
		buf := make([]byte, size)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out = append(out, buf...)
		if _, err := r.Discard(2); err != nil {
			return nil, err
		}
	}
}

func etagOf(data []byte) string {
	sum := md5.Sum(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
