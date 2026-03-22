package main

import (
	"os"
	"reflect"
	"testing"
)

func TestSplitCSV_TrimsAndDropsEmptyParts(t *testing.T) {
	got := splitCSV(" http://a:1, ,http://b:2 ,, http://c:3 ")
	want := []string{"http://a:1", "http://b:2", "http://c:3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected splitCSV output: got %#v want %#v", got, want)
	}
}

func TestEnvOr_UsesEnvironmentWhenPresent(t *testing.T) {
	const key = "ELKGO_TESTDATA_MAIN_ENV"
	if err := os.Setenv(key, "from-env"); err != nil {
		t.Fatalf("set env: %v", err)
	}
	defer os.Unsetenv(key)

	if got := envOr(key, "fallback"); got != "from-env" {
		t.Fatalf("expected env value, got %q", got)
	}

	if err := os.Setenv(key, "   "); err != nil {
		t.Fatalf("set blank env: %v", err)
	}
	if got := envOr(key, "fallback"); got != "fallback" {
		t.Fatalf("expected fallback for blank env, got %q", got)
	}
}
