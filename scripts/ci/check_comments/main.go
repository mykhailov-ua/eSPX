package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
)

var (
	bannedWord  = regexp.MustCompile(`(?i)\b(simple|elegant|clean|obviously|just|simply|nice|obvious|trivial)\b`)
	unicodeDash = regexp.MustCompile(`[—–]`)
)

func main() {
	roots := []string{"internal", "pkg", "cmd"}
	var failed bool

	for _, root := range roots {
		if _, err := os.Stat(root); os.IsNotExist(err) {
			continue
		}
		_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				base := filepath.Base(path)
				if base == "pb" || base == "db" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			for _, v := range scanFile(path) {
				failed = true
				fmt.Fprintf(os.Stderr, "%s:%d: %s\n", path, v.line, v.msg)
			}
			return nil
		})
	}

	if failed {
		os.Exit(1)
	}
	fmt.Println("check_comments: ok")
}

type violation struct {
	line int
	msg  string
}

func scanFile(path string) []violation {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return []violation{{line: 1, msg: "parse error: " + err.Error()}}
	}

	var out []violation
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			text := commentBody(c.Text)
			if text == "" {
				continue
			}
			pos := fset.Position(c.Pos())
			if v := checkCommentText(text); v != "" {
				out = append(out, violation{line: pos.Line, msg: v})
			}
		}
	}
	return out
}

func commentBody(raw string) string {
	text := strings.TrimSpace(raw)
	text = strings.TrimPrefix(text, "//")
	text = strings.TrimPrefix(text, "/*")
	text = strings.TrimSuffix(text, "*/")
	text = strings.TrimSpace(strings.TrimPrefix(text, "*"))
	return text
}

func checkCommentText(text string) string {
	if bannedWord.MatchString(text) {
		return "banned word in comment"
	}
	if unicodeDash.MatchString(text) {
		return "unicode dash in comment (use ASCII -)"
	}
	for _, r := range text {
		if r < 0x20 || r > 0x7e {
			if r != '\t' && !unicode.IsSpace(r) {
				return "non-ASCII in comment"
			}
		}
	}
	return ""
}
