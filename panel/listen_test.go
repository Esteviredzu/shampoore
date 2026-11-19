package panel

import "testing"

func TestValidateAdminListen(t *testing.T) {
	cases := []struct {
		addr        string
		allowPublic bool
		wantErr     bool
	}{
		{"127.0.0.1:8080", false, false},
		{"localhost:8080", false, false},
		{"[::1]:8080", false, false},
		{":8080", false, true},
		{"0.0.0.0:8080", false, true},
		{"203.0.113.10:8080", false, true},
		{"203.0.113.10:8080", true, false},
		{"", false, true},
		{"127.0.0.1", false, true},
	}
	for _, tc := range cases {
		err := ValidateAdminListen(tc.addr, tc.allowPublic)
		if tc.wantErr && err == nil {
			t.Fatalf("ValidateAdminListen(%q, %v) = nil, want error", tc.addr, tc.allowPublic)
		}
		if !tc.wantErr && err != nil {
			t.Fatalf("ValidateAdminListen(%q, %v) = %v", tc.addr, tc.allowPublic, err)
		}
	}
}
