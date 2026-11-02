package dump

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPostProcessDoesNotInterpretPathsAsCommands(t *testing.T) {
	root := t.TempDir()
	// These shell metacharacters must remain literal parts of directory names.
	directory := filepath.Join(root, "space ; $(touch INJECTED) `touch PWNED`")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(directory, "test.data.000000000.sql")
	schema := filepath.Join(directory, "test-schema.sql")
	for _, file := range []string{source, schema} {
		if err := os.WriteFile(file, []byte("header\nheader2\nline1\nline2\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := postProcessDump(directory); err != nil {
		t.Fatal(err)
	}
	for file, expected := range map[string]string{
		filepath.Join(directory, "test.data.sql"): "line1line2",
		schema: "line1\nline2\n",
	} {
		actual, err := os.ReadFile(file)
		if err != nil || string(actual) != expected {
			t.Fatalf("postprocessing: got %q, error %v", actual, err)
		}
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatalf("original data file was not removed: %v", err)
	}
}
