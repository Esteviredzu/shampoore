package control

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/Esteviredzu/shampoore-core/internal/server/manager"
)

type Client struct {
	path string
}

func NewClient(path string) *Client { return &Client{path: path} }

func (c *Client) Path() string { return c.path }

func (c *Client) call(req Request) (*Response, error) {
	conn, err := net.DialTimeout("unix", c.path, 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", c.path, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, err
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return &resp, fmt.Errorf("%s", resp.Error)
	}
	return &resp, nil
}

func (c *Client) Shutdown() error {
	_, err := c.call(Request{Cmd: "shutdown"})
	return err
}

func (c *Client) Status() (map[string]any, error) {
	r, err := c.call(Request{Cmd: "status"})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	_ = json.Unmarshal(r.Data, &out)
	return out, nil
}

func (c *Client) List() ([]manager.EntryView, error) {
	r, err := c.call(Request{Cmd: "list"})
	if err != nil {
		return nil, err
	}
	var out []manager.EntryView
	_ = json.Unmarshal(r.Data, &out)
	return out, nil
}

func (c *Client) Get(name string) (manager.EntryView, error) {
	r, err := c.call(Request{Cmd: "get", Name: name})
	if err != nil {
		return manager.EntryView{}, err
	}
	var out manager.EntryView
	_ = json.Unmarshal(r.Data, &out)
	return out, nil
}

func (c *Client) Add(name, template, content string) error {
	_, err := c.call(Request{Cmd: "add", Name: name, Template: template, Content: content})
	return err
}

func (c *Client) Update(name string, entry manager.ConfigEntry) error {
	_, err := c.call(Request{Cmd: "update", Name: name, Entry: &entry})
	return err
}

func (c *Client) Remove(name string) error {
	_, err := c.call(Request{Cmd: "remove", Name: name})
	return err
}

func (c *Client) SetEnabled(name string, on bool) error {
	cmd := "enable"
	if !on {
		cmd = "disable"
	}
	_, err := c.call(Request{Cmd: cmd, Name: name})
	return err
}

func (c *Client) Restart(name string) error {
	_, err := c.call(Request{Cmd: "restart", Name: name})
	return err
}

func (c *Client) Logs(name string, limit int) ([]string, error) {
	r, err := c.call(Request{Cmd: "logs", Name: name, Limit: limit})
	if err != nil {
		return nil, err
	}
	var out struct {
		Lines []string `json:"lines"`
	}
	_ = json.Unmarshal(r.Data, &out)
	return out.Lines, nil
}