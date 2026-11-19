package app

import "testing"

func TestIntStrict(t *testing.T) {
	v := Values{
		"int":    42,
		"float":  float64(17),
		"string": "23",
		"bad":    "abc",
		"frac":   1.5,
		"bool":   true,
	}

	if n, ok, err := v.IntStrict("int"); err != nil || !ok || n != 42 {
		t.Fatalf("int: got n=%d ok=%v err=%v", n, ok, err)
	}
	if n, ok, err := v.IntStrict("float"); err != nil || !ok || n != 17 {
		t.Fatalf("float: got n=%d ok=%v err=%v", n, ok, err)
	}
	if n, ok, err := v.IntStrict("string"); err != nil || !ok || n != 23 {
		t.Fatalf("string: got n=%d ok=%v err=%v", n, ok, err)
	}
	if _, ok, err := v.IntStrict("missing"); err != nil || ok {
		t.Fatalf("missing: ok=%v err=%v", ok, err)
	}
	for _, key := range []string{"bad", "frac", "bool"} {
		if _, _, err := v.IntStrict(key); err == nil {
			t.Fatalf("IntStrict(%q): expected error", key)
		}
	}

	if got := v.Int("bad", 7); got != 7 {
		t.Fatalf("lenient Int fallback = %d, want 7", got)
	}
}

func TestBoolStrict(t *testing.T) {
	v := Values{
		"bool":  true,
		"yes":   "yes",
		"off":   "off",
		"bad":   "maybe",
		"count": 1,
	}
	if b, ok, err := v.BoolStrict("bool"); err != nil || !ok || !b {
		t.Fatalf("bool: %v %v %v", b, ok, err)
	}
	if b, ok, err := v.BoolStrict("yes"); err != nil || !ok || !b {
		t.Fatalf("yes: %v %v %v", b, ok, err)
	}
	if b, ok, err := v.BoolStrict("off"); err != nil || !ok || b {
		t.Fatalf("off: %v %v %v", b, ok, err)
	}
	for _, key := range []string{"bad", "count"} {
		if _, _, err := v.BoolStrict(key); err == nil {
			t.Fatalf("BoolStrict(%q): expected error", key)
		}
	}
	if got := v.Bool("bad", true); !got {
		t.Fatalf("lenient Bool fallback = %v, want true", got)
	}
}

func TestStringStrict(t *testing.T) {
	v := Values{"s": "value", "n": 10}
	if s, ok, err := v.StringStrict("s"); err != nil || !ok || s != "value" {
		t.Fatalf("s: %q %v %v", s, ok, err)
	}
	if _, _, err := v.StringStrict("n"); err == nil {
		t.Fatal("StringStrict(number): expected error")
	}
	if got := v.String("n", "def"); got != "def" {
		t.Fatalf("lenient String fallback = %q, want def", got)
	}
}

func TestValidateTypes(t *testing.T) {
	valid := Values{
		"s":    "x",
		"n":    float64(1),
		"b":    true,
		"list": []any{"a", "b"},
	}
	if err := valid.ValidateTypes(); err != nil {
		t.Fatalf("valid values rejected: %v", err)
	}

	invalid := []Values{
		{"nil": nil},
		{"": "x"},
		{"obj": map[string]any{"a": 1}},
		{"list": []any{"a", 1}},
	}
	for i, v := range invalid {
		if err := v.ValidateTypes(); err == nil {
			t.Fatalf("invalid values %d accepted", i)
		}
	}
}

func TestValidateAgainstDefaults(t *testing.T) {
	defaults := Values{"num": 1, "flag": true, "text": "x"}
	if err := (Values{"num": "10", "flag": "yes", "text": "y"}).ValidateAgainstDefaults(defaults); err != nil {
		t.Fatalf("compatible values rejected: %v", err)
	}
	if err := (Values{"num": "abc"}).ValidateAgainstDefaults(defaults); err == nil {
		t.Fatal("expected error for non-numeric value")
	}
	if err := (Values{"flag": "maybe"}).ValidateAgainstDefaults(defaults); err == nil {
		t.Fatal("expected error for non-boolean value")
	}
	if err := (Values{"text": 5}).ValidateAgainstDefaults(defaults); err == nil {
		t.Fatal("expected error for non-string value")
	}
}
