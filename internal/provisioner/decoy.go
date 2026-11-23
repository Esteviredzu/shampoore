package provisioner

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

const (
	decoyDownloadTimeout = 20 * time.Second
	decoyMaxArchiveSize  = 15 << 20
	decoyMaxExtracted    = 64 << 20

	decoyPlaceholder = "decoy/.keep"

	decoyExtractPrefix = "decoy/"
)

func decoyRepoURL(node panelstore.Node) string {
	for _, tr := range node.Transports {
		if !tr.Enabled {
			continue
		}
		descriptor, ok := app.DescriptorFor(tr.Type)
		if !ok || !descriptor.SupportsDecoy() {
			continue
		}
		if raw := strings.TrimSpace(tr.DecoyRepoURL); raw != "" {
			return raw
		}
	}
	return ""
}

func decoyArchiveURLs(repoURL string) ([]string, error) {
	repoURL = strings.TrimSpace(repoURL)
	if repoURL == "" {
		return nil, errors.New("provisioner: decoy repo URL is empty")
	}
	parsed, err := url.Parse(repoURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("provisioner: invalid decoy repo URL %q", repoURL)
	}
	if strings.HasSuffix(strings.ToLower(parsed.Path), ".zip") {
		return []string{parsed.String()}, nil
	}
	base := strings.TrimSuffix(parsed.Path, "/")
	urls := make([]string, 0, 2)
	for _, branch := range []string{"main", "master"} {
		next := *parsed
		next.Path = base + "/archive/refs/heads/" + branch + ".zip"
		urls = append(urls, next.String())
	}
	return urls, nil
}

func downloadDecoySite(ctx context.Context, client *http.Client, repoURL string) ([]byte, error) {
	urls, err := decoyArchiveURLs(repoURL)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{Timeout: decoyDownloadTimeout}
	}
	ctx, cancel := context.WithTimeout(ctx, decoyDownloadTimeout)
	defer cancel()

	var lastErr error
	for _, archiveURL := range urls {
		data, status, err := fetchDecoyArchive(ctx, client, archiveURL)
		if err != nil {
			return nil, err
		}
		switch status {
		case http.StatusOK:
			return data, nil
		case http.StatusNotFound:
			lastErr = fmt.Errorf("provisioner: decoy archive %s not found", archiveURL)
		default:
			return nil, fmt.Errorf("provisioner: download decoy archive %s: status %d", archiveURL, status)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("provisioner: decoy archive for %q not found", repoURL)
	}
	return nil, lastErr
}

func fetchDecoyArchive(ctx context.Context, client *http.Client, archiveURL string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, archiveURL, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("provisioner: build decoy request: %w", err)
	}
	req.Header.Set("User-Agent", "shampoore-panel")
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("provisioner: download decoy archive: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.CopyN(io.Discard, resp.Body, 1<<10)
		return nil, resp.StatusCode, nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, decoyMaxArchiveSize+1))
	if err != nil {
		return nil, 0, fmt.Errorf("provisioner: read decoy archive: %w", err)
	}
	if len(data) > decoyMaxArchiveSize {
		return nil, 0, fmt.Errorf("provisioner: decoy archive exceeds %d bytes", decoyMaxArchiveSize)
	}
	return data, http.StatusOK, nil
}

func extractDecoySite(data []byte) ([]File, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("provisioner: open decoy archive: %w", err)
	}
	files := make([]File, 0, len(zr.File))
	hasIndex := false
	total := 0
	for _, entry := range zr.File {
		if !entry.FileInfo().Mode().IsRegular() {
			continue
		}
		name := path.Clean(strings.TrimPrefix(entry.Name, "./"))
		if name == "." || name == ".." || strings.HasPrefix(name, "/") || strings.HasPrefix(name, "../") {
			return nil, fmt.Errorf("provisioner: decoy archive entry %q escapes the archive root", entry.Name)
		}
		first, rel, found := strings.Cut(name, "/")
		if !found || first == "" || rel == "" {

			continue
		}
		if err := validateArchiveName(rel); err != nil {
			return nil, fmt.Errorf("provisioner: decoy archive entry %q: %w", entry.Name, err)
		}
		rc, err := entry.Open()
		if err != nil {
			return nil, fmt.Errorf("provisioner: open decoy archive entry %q: %w", entry.Name, err)
		}
		payload, readErr := io.ReadAll(io.LimitReader(rc, int64(decoyMaxExtracted-total)+1))
		closeErr := rc.Close()
		if readErr != nil {
			return nil, fmt.Errorf("provisioner: read decoy archive entry %q: %w", entry.Name, readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("provisioner: close decoy archive entry %q: %w", entry.Name, closeErr)
		}
		total += len(payload)
		if total > decoyMaxExtracted {
			return nil, fmt.Errorf("provisioner: decoy archive expands beyond %d bytes", decoyMaxExtracted)
		}
		if rel == "index.html" {
			hasIndex = true
		}
		files = append(files, File{Name: decoyExtractPrefix + rel, Mode: 0o644, Data: payload})
	}
	if !hasIndex {
		return nil, errors.New("provisioner: decoy archive has no index.html in its root folder")
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	return files, nil
}

func (p *Provisioner) decoyFiles(ctx context.Context, node panelstore.Node) ([]File, error) {
	if !node.IsRelay {
		return nil, nil
	}
	repoURL := decoyRepoURL(node)
	if repoURL == "" {
		return []File{{Name: decoyPlaceholder, Mode: 0o644}}, nil
	}
	data, err := downloadDecoySite(ctx, p.opts.HTTPClient, repoURL)
	if err != nil {
		return nil, err
	}
	files, err := extractDecoySite(data)
	if err != nil {
		return nil, err
	}
	return files, nil
}
