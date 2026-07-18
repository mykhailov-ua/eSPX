package licensing

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	// DefaultMaxJSONBytes caps cold-path admin JSON bodies for subscription/licensing endpoints.
	DefaultMaxJSONBytes = 64 * 1024
)

var (
	ErrJSONTooLarge    = errors.New("json payload exceeds max size")
	ErrJSONMalformed   = errors.New("json payload malformed")
	ErrJSONDisallowed  = errors.New("json contains disallowed constructs")
	ErrEmptyJSONBody   = errors.New("json body is empty")
)

// DecodeJSONStrict reads up to maxBytes from r and unmarshals into dst with DisallowUnknownFields.
func DecodeJSONStrict(r io.Reader, maxBytes int64, dst any) error {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxJSONBytes
	}
	limited := io.LimitReader(r, maxBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read json body: %w", err)
	}
	if int64(len(body)) > maxBytes {
		return ErrJSONTooLarge
	}
	if len(body) == 0 {
		return ErrEmptyJSONBody
	}
	if err := validateJSONSurface(body); err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: %v", ErrJSONMalformed, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: trailing data after first value", ErrJSONMalformed)
	}
	return nil
}

func validateJSONSurface(body []byte) error {
	// Reject obviously hostile constructs before full parse (depth bombs, duplicate keys via raw scan).
	if strings.Contains(string(body), "\x00") {
		return fmt.Errorf("%w: null byte in payload", ErrJSONDisallowed)
	}
	depth := 0
	for _, c := range body {
		switch c {
		case '{', '[':
			depth++
			if depth > 32 {
				return fmt.Errorf("%w: nesting depth exceeds limit", ErrJSONDisallowed)
			}
		case '}', ']':
			depth--
			if depth < 0 {
				return fmt.Errorf("%w: unbalanced brackets", ErrJSONMalformed)
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("%w: unclosed brackets", ErrJSONMalformed)
	}
	return nil
}
