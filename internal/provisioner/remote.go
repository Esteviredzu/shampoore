package provisioner

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/Esteviredzu/shampoore/internal/panelstore"
)

type File struct {
	Name string
	Mode int64
	Data []byte
}

type Remote interface {
	Run(ctx context.Context, command string) (string, error)
	Upload(ctx context.Context, dir string, files []File, sudo bool) error
	Close() error
}

type sshRemote struct {
	client *ssh.Client
}

func dialSSH(ctx context.Context, node panelstore.Node, hostKey ssh.HostKeyCallback) (Remote, error) {
	var auths []ssh.AuthMethod
	if strings.TrimSpace(node.SSHPrivateKey) != "" {
		signer, err := ssh.ParsePrivateKey([]byte(node.SSHPrivateKey))
		if err != nil {
			if strings.TrimSpace(node.SSHPassword) == "" {
				return nil, fmt.Errorf("provisioner: parse private key: %w", err)
			}
		} else {
			auths = append(auths, ssh.PublicKeys(signer))
		}
	}
	if strings.TrimSpace(node.SSHPassword) != "" {
		auths = append(auths, ssh.Password(node.SSHPassword))
	}
	if len(auths) == 0 {
		return nil, errors.New("provisioner: no ssh authentication method configured")
	}
	if hostKey == nil {
		hostKey = ssh.InsecureIgnoreHostKey()
	}

	addr := net.JoinHostPort(node.SSHHost, strconv.Itoa(node.SSHPort))
	dialer := net.Dialer{Timeout: 15 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("provisioner: dial %s: %w", addr, err)
	}

	cfg := &ssh.ClientConfig{
		User:            node.SSHUser,
		Auth:            auths,
		HostKeyCallback: hostKey,
		Timeout:         15 * time.Second,
	}
	cc, chans, reqs, err := ssh.NewClientConn(conn, addr, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("provisioner: ssh handshake %s: %w", addr, err)
	}
	return &sshRemote{client: ssh.NewClient(cc, chans, reqs)}, nil
}

func (r *sshRemote) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}

func (r *sshRemote) Run(ctx context.Context, command string) (string, error) {
	session, err := r.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("provisioner: ssh session: %w", err)
	}
	defer session.Close()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = session.Close()
		case <-done:
		}
	}()
	out, err := session.CombinedOutput(command)
	close(done)
	if ctx.Err() != nil {
		return string(out), ctx.Err()
	}
	if err != nil {
		return string(out), fmt.Errorf("command %q: %w", command, err)
	}
	return string(out), nil
}

func (r *sshRemote) Upload(ctx context.Context, dir string, files []File, sudo bool) error {

	var archive bytes.Buffer
	if err := writeUploadArchive(&archive, files); err != nil {
		return fmt.Errorf("provisioner: build upload archive: %w", err)
	}

	session, err := r.client.NewSession()
	if err != nil {
		return fmt.Errorf("provisioner: ssh upload session: %w", err)
	}
	defer session.Close()

	session.Stdin = bytes.NewReader(archive.Bytes())
	var stderr bytes.Buffer
	session.Stderr = &stderr

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = session.Close()
		case <-done:
		}
	}()

	prefix := ""
	if sudo {
		prefix = "sudo -n "
	}
	cmd := prefix + "mkdir -p " + shellQuote(dir) + " && " + prefix + "tar -xzf - -C " + shellQuote(dir)
	err = session.Run(cmd)
	close(done)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("provisioner: upload: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func writeUploadArchive(w io.Writer, files []File) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)

	err := func() error {
		for _, f := range files {
			if err := validateArchiveName(f.Name); err != nil {
				return fmt.Errorf("file %q: %w", f.Name, err)
			}
			hdr := &tar.Header{
				Name:     f.Name,
				Mode:     f.Mode,
				Size:     int64(len(f.Data)),
				Typeflag: tar.TypeReg,
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return fmt.Errorf("file %q: write header: %w", f.Name, err)
			}
			if _, err := tw.Write(f.Data); err != nil {
				return fmt.Errorf("file %q: write data: %w", f.Name, err)
			}
		}
		return nil
	}()
	if closeErr := tw.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("close tar: %w", closeErr)
	}
	if closeErr := gz.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("close gzip: %w", closeErr)
	}
	return err
}

func validateArchiveName(name string) error {
	if name == "" {
		return errors.New("empty name")
	}
	if strings.HasPrefix(name, "/") {
		return fmt.Errorf("absolute path %q is not allowed", name)
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("path %q escapes the upload directory", name)
	}
	return nil
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
