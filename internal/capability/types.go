package capability

type SecretGenerator struct {
	Kind   string `json:"kind"`
	Bytes  int    `json:"bytes,omitempty"`
	Prefix string `json:"prefix,omitempty"`
}

type SecretSpec struct {
	Key       string          `json:"key"`
	Label     string          `json:"label"`
	Generator SecretGenerator `json:"generator"`
	Aliases   []string        `json:"aliases,omitempty"`
	Editable  bool            `json:"editable"`
}

type Asset struct {
	Name string
	Mode int64
	Data []byte
}

type NodeStep struct {
	Name    string
	Command string
}

type RelayContext struct {
	NodeID     int64
	NodeName   string
	Domain     string
	LocalRelay bool
	Secrets    map[string]string
}

type RelayAsset struct {
	Name      string
	Upstream  string
	DependsOn []string
	Compose   []byte
	Files     []Asset
	Steps     []NodeStep
	Domains   []string
}
