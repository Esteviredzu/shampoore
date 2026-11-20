package provisioner

import (
	"archive/zip"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func buildZipArchive(t *testing.T, entries map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, data := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("zip create %q: %v", name, err)
		}
		if _, err := w.Write([]byte(data)); err != nil {
			t.Fatalf("zip write %q: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func newDecoyHTTPServer(t *testing.T, mainZip, masterZip []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/archive/refs/heads/main.zip") && mainZip != nil:
			_, _ = w.Write(mainZip)
		case strings.HasSuffix(r.URL.Path, "/archive/refs/heads/master.zip") && masterZip != nil:
			_, _ = w.Write(masterZip)
		case strings.HasSuffix(r.URL.Path, "/site.zip") && mainZip != nil:
			_, _ = w.Write(mainZip)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestDecoyArchiveURLs(t *testing.T) {
	urls, err := decoyArchiveURLs("https://github.com/owner/repo")
	if err != nil {
		t.Fatalf("archive urls: %v", err)
	}
	want := []string{
		"https://github.com/owner/repo/archive/refs/heads/main.zip",
		"https://github.com/owner/repo/archive/refs/heads/master.zip",
	}
	if len(urls) != len(want) {
		t.Fatalf("urls = %v, want %v", urls, want)
	}
	for i := range want {
		if urls[i] != want[i] {
			t.Fatalf("urls[%d] = %q, want %q", i, urls[i], want[i])
		}
	}

	direct, err := decoyArchiveURLs("https://codeload.github.com/owner/repo/zip/refs/heads/main.zip")
	if err != nil {
		t.Fatalf("direct archive urls: %v", err)
	}
	if len(direct) != 1 || direct[0] != "https://codeload.github.com/owner/repo/zip/refs/heads/main.zip" {
		t.Fatalf("direct urls = %v", direct)
	}

	if _, err := decoyArchiveURLs(""); err == nil {
		t.Fatal("empty repo URL must fail")
	}
	if _, err := decoyArchiveURLs("github.com/owner/repo"); err == nil {
		t.Fatal("relative repo URL must fail")
	}
}

func TestDownloadDecoySiteUsesMainBranch(t *testing.T) {
	mainZip := buildZipArchive(t, map[string]string{
		"example_website-main/index.html": "<html>main</html>",
	})
	srv := newDecoyHTTPServer(t, mainZip, nil)

	data, err := downloadDecoySite(context.Background(), srv.Client(), srv.URL+"/owner/repo")
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	files, err := extractDecoySite(data)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if len(files) != 1 || files[0].Name != "decoy/index.html" || string(files[0].Data) != "<html>main</html>" {
		t.Fatalf("files = %+v", files)
	}
}

func TestDownloadDecoySiteFallsBackToMaster(t *testing.T) {
	masterZip := buildZipArchive(t, map[string]string{
		"example_website-master/index.html":    "<html>master</html>",
		"example_website-master/style.css":     "body{margin:0}",
		"example_website-master/assets/app.js": "console.log('site')",
	})
	srv := newDecoyHTTPServer(t, nil, masterZip)

	data, err := downloadDecoySite(context.Background(), srv.Client(), srv.URL+"/owner/repo")
	if err != nil {
		t.Fatalf("download fallback: %v", err)
	}
	files, err := extractDecoySite(data)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	got := fileMap(files)
	for _, name := range []string{"decoy/index.html", "decoy/style.css", "decoy/assets/app.js"} {
		if _, ok := got[name]; !ok {
			t.Fatalf("missing %q in %+v", name, got)
		}
	}
	if string(got["decoy/assets/app.js"].Data) != "console.log('site')" {
		t.Fatalf("nested asset data = %q", got["decoy/assets/app.js"].Data)
	}
}

func TestDownloadDecoySiteDirectZipLink(t *testing.T) {
	zipData := buildZipArchive(t, map[string]string{
		"repo-main/index.html": "<html>direct</html>",
	})
	srv := newDecoyHTTPServer(t, zipData, nil)

	data, err := downloadDecoySite(context.Background(), srv.Client(), srv.URL+"/site.zip")
	if err != nil {
		t.Fatalf("download direct zip: %v", err)
	}
	files, err := extractDecoySite(data)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if len(files) != 1 || files[0].Name != "decoy/index.html" {
		t.Fatalf("files = %+v", files)
	}
}

func TestDownloadDecoySiteReportsMissingArchive(t *testing.T) {
	srv := newDecoyHTTPServer(t, nil, nil)
	if _, err := downloadDecoySite(context.Background(), srv.Client(), srv.URL+"/owner/repo"); err == nil {
		t.Fatal("expected error for missing archive")
	}
}

func TestExtractDecoySiteRejectsMissingIndex(t *testing.T) {
	zipData := buildZipArchive(t, map[string]string{
		"repo-main/style.css": "body{}",
	})
	if _, err := extractDecoySite(zipData); err == nil {
		t.Fatal("expected missing index.html error")
	}
}

func TestExtractDecoySiteRejectsTraversal(t *testing.T) {
	zipData := buildZipArchive(t, map[string]string{
		"repo-main/index.html":     "<html>ok</html>",
		"repo-main/../../evil.txt": "escape",
	})
	files, err := extractDecoySite(zipData)
	if err == nil {
		t.Fatalf("expected traversal error, got files %+v", files)
	}
	if !strings.Contains(err.Error(), "escapes") && !strings.Contains(err.Error(), "upload directory") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExtractDecoySiteRejectsBrokenArchive(t *testing.T) {
	if _, err := extractDecoySite([]byte("not a zip")); err == nil {
		t.Fatal("expected broken archive error")
	}
}
