package internal_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const modulePrefix = "github.com/code-corhuila/multi-tour-workflow-onboard-tenant/internal/"

// The domain and the application may import the standard library and each other only (norm §5.3).
func TestC_CoreImportsOnlyTheStandardLibrary(t *testing.T) {
	fset := token.NewFileSet()
	parsed := 0
	for _, root := range []string{"domain", "application"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
				return err
			}
			file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			parsed++
			for _, spec := range file.Imports {
				imported, _ := strconv.Unquote(spec.Path.Value)
				first, _, _ := strings.Cut(imported, "/")
				stdlib := !strings.Contains(first, ".")
				inner := strings.HasPrefix(imported, modulePrefix+"domain") || strings.HasPrefix(imported, modulePrefix+"application")
				if !stdlib && !inner {
					t.Errorf("%s imports %s", path, imported)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if parsed == 0 {
		t.Fatal("no source file was read")
	}
}
