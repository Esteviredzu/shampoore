package panelstore

import "github.com/Esteviredzu/shampoore/internal/capability"

func BaseSecretSpecs() []capability.SecretSpec {
	return []capability.SecretSpec{
		{
			Key:       "encryption_password",
			Label:     "Encryption Password",
			Generator: capability.SecretGenerator{Kind: "hex", Bytes: 32},
			Editable:  true,
		},
		{
			Key:       "tunnel_id",
			Label:     "Tunnel ID",
			Generator: capability.SecretGenerator{Kind: "literal", Prefix: "shampoore"},
			Editable:  false,
		},
	}
}

type LegacyPolicy struct{}

func (LegacyPolicy) Known(kind string) bool {
	switch kind {
	case TransportRawWS, TransportSilo, TransportLiveKit, TransportMulti:
		return true
	default:
		return false
	}
}

func (LegacyPolicy) IsMulti(kind string) bool { return kind == TransportMulti }

func (LegacyPolicy) MultiKinds() []string { return []string{TransportMulti} }

func (LegacyPolicy) MultiStrategies(kind string) []string {
	if kind != TransportMulti {
		return nil
	}
	return []string{MultiStrategyBond, MultiStrategyFlowMux}
}

func (LegacyPolicy) AllowsDecoy(kind string) bool { return kind == TransportRawWS }

func (LegacyPolicy) Secrets() []capability.SecretSpec {
	specs := BaseSecretSpecs()
	specs = append(specs,
		capability.SecretSpec{
			Key:       "raw_ws_password",
			Label:     "Raw WS Password",
			Generator: capability.SecretGenerator{Kind: "hex", Bytes: 32},
			Editable:  true,
		},
		capability.SecretSpec{
			Key:       "livekit_api_key",
			Label:     "LiveKit API Key",
			Generator: capability.SecretGenerator{Kind: "hex", Bytes: 8},
			Editable:  true,
		},
		capability.SecretSpec{
			Key:       "livekit_api_secret",
			Label:     "LiveKit API Secret",
			Generator: capability.SecretGenerator{Kind: "base64", Bytes: 32},
			Editable:  true,
		},
		capability.SecretSpec{
			Key:       "minio_root_user",
			Label:     "MinIO Root User",
			Generator: capability.SecretGenerator{Kind: "hex", Bytes: 6, Prefix: "shampoore-"},
			Aliases:   []string{"s3_access_key"},
			Editable:  true,
		},
		capability.SecretSpec{
			Key:       "minio_root_password",
			Label:     "MinIO Root Password",
			Generator: capability.SecretGenerator{Kind: "hex", Bytes: 24},
			Aliases:   []string{"s3_secret_key"},
			Editable:  true,
		},
		capability.SecretSpec{
			Key:       "s3_bucket",
			Label:     "S3 Bucket",
			Generator: capability.SecretGenerator{Kind: "hex", Bytes: 4, Prefix: "shampoore-"},
			Editable:  true,
		},
	)
	return specs
}

func (p LegacyPolicy) EditableSecret(key string) bool {
	for _, spec := range p.Secrets() {
		if spec.Key == key {
			return spec.Editable
		}
	}
	return false
}
