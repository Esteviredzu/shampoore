package manager

import (
	"testing"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/auth"
	"github.com/Esteviredzu/shampoore/internal/logging"
)

func TestUsageAggregatesUserRegistries(t *testing.T) {
	registry := auth.NewRegistry()
	if err := registry.Upsert(auth.UserSpec{UserID: "alice", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := registry.AddTraffic("alice", 10, 20); err != nil {
		t.Fatal(err)
	}
	m := New("", logging.New("test", false), nil, false)
	m.entries["route-1"] = &ManagedEntry{
		Name: "route-1",
		rt:   &app.Runtime{UserRegistry: registry},
	}
	usage := m.Usage()
	if len(usage) != 1 {
		t.Fatalf("usage = %+v", usage)
	}
	if usage[0].Config != "route-1" || usage[0].UserID != "alice" ||
		usage[0].UploadBytes != 10 || usage[0].DownloadBytes != 20 {
		t.Fatalf("usage entry = %+v", usage[0])
	}
}
