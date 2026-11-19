package app

import (
	"context"
	"testing"

	"github.com/Esteviredzu/shampoore/internal/logging"
)

func testDescriptor(kind string, aliases ...string) Descriptor {
	return Descriptor{
		Kind:    kind,
		Aliases: aliases,
		Build: func(context.Context, *Settings, string, logging.ContextLogger, Values) (*BuildResult, error) {
			return &BuildResult{}, nil
		},
	}
}

func TestDescriptorAliases(t *testing.T) {
	const kind = "test_alias_transport"
	const alias = "test_alias_short"
	if err := RegisterDescriptor(testDescriptor(kind, alias)); err != nil {
		t.Fatalf("register: %v", err)
	}
	byKind, ok := DescriptorFor(kind)
	if !ok || byKind.Kind != kind {
		t.Fatalf("DescriptorFor(kind) = %+v, %v", byKind, ok)
	}
	byAlias, ok := DescriptorFor(alias)
	if !ok || byAlias.Kind != kind {
		t.Fatalf("DescriptorFor(alias) = %+v, %v", byAlias, ok)
	}
	if _, ok := DescriptorFor("test_alias_missing"); ok {
		t.Fatal("unknown descriptor resolved")
	}
}

func TestDescriptorAliasConflict(t *testing.T) {
	const alias = "test_alias_conflict"
	if err := RegisterDescriptor(testDescriptor("test_alias_owner", alias)); err != nil {
		t.Fatalf("register owner: %v", err)
	}
	if err := RegisterDescriptor(testDescriptor("test_alias_intruder", alias)); err == nil {
		t.Fatal("duplicate alias accepted")
	}
}
