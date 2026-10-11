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

// Hexagonal barrier (norm §5.3): the domain imports only the standard library; the application imports
// the standard library, the domain and itself.
func TestC_CoreImportsOnlyTheStandardLibrary(t *testing.T) {
	rules := map[string][]string{
		"domain":      nil,
		"application": {modulePrefix + "domain", modulePrefix + "application"},
	}
	parsed := 0
	for root, allowed := range rules {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
				return err
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			parsed++
			for _, spec := range file.Imports {
				if imported, _ := strconv.Unquote(spec.Path.Value); !isAllowed(imported, allowed) {
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

func TestC_ImportRuleRejectsLookalikes(t *testing.T) {
	allowed := []string{modulePrefix + "domain"}
	cases := map[string]bool{
		"fmt":                               true,
		"net/http":                          true,
		modulePrefix + "domain":             true,
		modulePrefix + "domain/saga":        true,
		modulePrefix + "domainx":            false,
		modulePrefix + "adapter/in/httpapi": false,
		"github.com/golang-jwt/jwt/v5":      false,
		"C":                                 false,
	}
	for imported, want := range cases {
		if got := isAllowed(imported, allowed); got != want {
			t.Errorf("isAllowed(%q) = %v, want %v", imported, got, want)
		}
	}
}

// A standard library path has no dot in its first element; an inner package matches on the path boundary.
func isAllowed(imported string, inner []string) bool {
	if imported == "C" {
		return false
	}
	if first, _, _ := strings.Cut(imported, "/"); !strings.Contains(first, ".") {
		return true
	}
	for _, prefix := range inner {
		if imported == prefix || strings.HasPrefix(imported, prefix+"/") {
			return true
		}
	}
	return false
}
