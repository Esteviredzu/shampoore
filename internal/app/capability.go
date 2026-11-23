package app

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/capability"
)

type RouteRole string

const (
	RouteRoleSubscription RouteRole = "subscription"

	RouteRoleExit RouteRole = "exit"

	RouteRoleExitSubflow RouteRole = "exit_subflow"
)

type ClientConfigProvider interface {
	SubscriptionConfig(RouteContext) (Settings, error)
}

type ServerConfigProvider interface {
	ExitConfig(RouteContext) (Settings, error)
	ExitSubflowConfig(RouteContext) (Settings, error)
}

type RelayProvider interface {
	RelayUpstream() string
	RelaySecrets() []string
	RelayAsset(capability.RelayContext) (capability.RelayAsset, error)
	SupportsDecoy() bool
}

type MultiuserDescriptor interface {
	SupportsMultiuser() bool
	PatchUsers(json.RawMessage, []auth.UserSpec) (json.RawMessage, bool, error)
}

type LinkCodec interface {
	DecodeQuery(url.Values) (json.RawMessage, error)
	EncodeQuery(json.RawMessage, url.Values) error
}

type SecretProvider interface {
	SecretSpecs() []capability.SecretSpec
}

type SecretProviderFunc func() []capability.SecretSpec

func (f SecretProviderFunc) SecretSpecs() []capability.SecretSpec { return f() }

type MultiStrategy struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type SubflowSpec struct {
	Type        string
	Name        string
	Transport   json.RawMessage
	Advanced    Values
	RateLimitKB int
}

type MultiRequest struct {
	Strategy           string
	RouteID            int64
	RouteName          string
	UserID             string
	ExitNodeID         string
	EncryptionPassword string
	Subflows           []SubflowSpec
}

type MultiComposer interface {
	Strategies() []MultiStrategy
	Validate(MultiRequest) error
	BuildSubscription(MultiRequest) (Settings, error)
	BuildExit(MultiRequest) (Settings, error)
}

type Capabilities struct {
	Client    ClientConfigProvider
	Server    ServerConfigProvider
	Relay     RelayProvider
	Multiuser MultiuserDescriptor
	Link      LinkCodec
	Secrets   SecretProvider
	Multi     MultiComposer
}

type ClientConfigFunc func(RouteContext) (Settings, error)

func (f ClientConfigFunc) SubscriptionConfig(ctx RouteContext) (Settings, error) {
	return f(ctx)
}

type ServerConfigAdapter struct {
	ExitFn    func(RouteContext) (Settings, error)
	SubflowFn func(RouteContext) (Settings, error)
}

func (a ServerConfigAdapter) ExitConfig(ctx RouteContext) (Settings, error) {
	if a.ExitFn == nil {
		return Settings{}, fmt.Errorf("app: exit config builder is not configured")
	}
	return a.ExitFn(ctx)
}

func (a ServerConfigAdapter) ExitSubflowConfig(ctx RouteContext) (Settings, error) {
	if a.SubflowFn != nil {
		return a.SubflowFn(ctx)
	}
	return a.ExitConfig(ctx)
}

type legacyLinkCodec struct {
	decode func(url.Values) (json.RawMessage, error)
	encode func(json.RawMessage, url.Values) error
}

func (l legacyLinkCodec) DecodeQuery(q url.Values) (json.RawMessage, error) {
	if l.decode == nil {
		return nil, fmt.Errorf("app: url decoding is not configured")
	}
	return l.decode(q)
}

func (l legacyLinkCodec) EncodeQuery(raw json.RawMessage, q url.Values) error {
	if l.encode == nil {
		return fmt.Errorf("app: url encoding is not configured")
	}
	return l.encode(raw, q)
}

type legacyRelayProvider struct {
	kind     string
	upstream string
	secrets  []string
}

func (l legacyRelayProvider) RelayUpstream() string { return l.upstream }
func (l legacyRelayProvider) RelaySecrets() []string {
	return append([]string(nil), l.secrets...)
}
func (l legacyRelayProvider) RelayAsset(capability.RelayContext) (capability.RelayAsset, error) {
	return capability.RelayAsset{}, fmt.Errorf("app: transport %q has no relay asset; implement app.RelayProvider", l.kind)
}
func (l legacyRelayProvider) SupportsDecoy() bool { return false }

func (d Descriptor) adaptLegacyCapabilities() Descriptor {
	if d.Caps.Client == nil && d.BuildSubscriptionConfig != nil {
		fn := d.BuildSubscriptionConfig
		d.Caps.Client = ClientConfigFunc(func(ctx RouteContext) (Settings, error) {
			return fn(ctx)
		})
	}
	if d.Caps.Server == nil && d.BuildServerConfig != nil {
		fn := d.BuildServerConfig
		d.Caps.Server = ServerConfigAdapter{
			ExitFn: func(ctx RouteContext) (Settings, error) {
				return fn(ctx)
			},
			SubflowFn: func(ctx RouteContext) (Settings, error) {
				return fn(ctx)
			},
		}
	}
	if d.Caps.Link == nil && (d.DecodeURL != nil || d.EncodeURL != nil) {
		d.Caps.Link = legacyLinkCodec{decode: d.DecodeURL, encode: d.EncodeURL}
	}
	if d.Caps.Relay == nil && d.RelayUpstream != "" {
		d.Caps.Relay = legacyRelayProvider{
			kind:     d.Kind,
			upstream: d.RelayUpstream,
			secrets:  d.RelaySecrets,
		}
	}
	return d
}

func (d Descriptor) ClientConfig(ctx RouteContext) (Settings, error) {
	if d.Caps.Client == nil {
		return Settings{}, fmt.Errorf("app: transport %q does not support subscription config", d.Kind)
	}
	return d.Caps.Client.SubscriptionConfig(ctx)
}

func (d Descriptor) ExitConfig(ctx RouteContext) (Settings, error) {
	if d.Caps.Server == nil {
		return Settings{}, fmt.Errorf("app: transport %q does not support exit config", d.Kind)
	}
	return d.Caps.Server.ExitConfig(ctx)
}

func (d Descriptor) ExitSubflowConfig(ctx RouteContext) (Settings, error) {
	if d.Caps.Server == nil {
		return Settings{}, fmt.Errorf("app: transport %q does not support exit subflow config", d.Kind)
	}
	return d.Caps.Server.ExitSubflowConfig(ctx)
}

func (d Descriptor) DecodeLink(q url.Values) (json.RawMessage, error) {
	if d.Caps.Link != nil {
		return d.Caps.Link.DecodeQuery(q)
	}
	if d.DecodeURL != nil {
		return d.DecodeURL(q)
	}
	return nil, fmt.Errorf("app: transport %q does not support URL parsing", d.Kind)
}

func (d Descriptor) EncodeLink(raw json.RawMessage, q url.Values) error {
	if d.Caps.Link != nil {
		return d.Caps.Link.EncodeQuery(raw, q)
	}
	if d.EncodeURL != nil {
		return d.EncodeURL(raw, q)
	}
	return fmt.Errorf("app: transport %q does not support URL encoding", d.Kind)
}

func (d Descriptor) RelayUpstreamAddr() string {
	if d.Caps.Relay != nil {
		return d.Caps.Relay.RelayUpstream()
	}
	return d.RelayUpstream
}

func (d Descriptor) SupportsDecoy() bool {
	return d.Caps.Relay != nil && d.Caps.Relay.SupportsDecoy()
}
