// Command benchguard reads a gateway's JSON log on stdin and fails unless
// every upstream the gateway enabled points at the mock. make bench-gateway
// runs it on gateway-bench's startup log before k6 starts, so a load test
// never reaches a real API.
//
//	docker logs <container> 2>&1 | go run ./cmd/benchguard -mock https://mockupstream:9090
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	mock := flag.String("mock", "", "the mock's scheme://host:port, such as https://mockupstream:9090")
	flag.Parse()
	if err := check(os.Stdin, *mock); err != nil {
		fmt.Fprintf(os.Stderr, "bench-gateway: %v; refusing to run\n", err)
		os.Exit(1)
	}
}

const enabledMsg = "upstream enabled"

// check reads gateway log lines from r. It returns nil only if the gateway
// logged "listening", logged at least one "upstream enabled" line, and
// every such line has exactly one base_url, equal to mock. The gateway logs
// base_url as scheme://host[:port], so the comparison is exact.
//
// It fails closed. Keys are compared ignoring case, as Go's JSON decoder
// matches them, and a key that appears more than once, where the decoder
// would keep the last, makes the line count as unreadable. A line that
// mentions "upstream enabled" but can't be read as one is an error. Other
// lines, JSON or not, are skipped. Errors name the upstream at fault but
// never its base URL.
func check(r io.Reader, mock string) error {
	if mock == "" {
		return errors.New("benchguard needs -mock")
	}
	var listening bool
	var enabled int
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 1<<20)
	for n := 1; sc.Scan(); n++ {
		raw := sc.Bytes()
		fields, _ := parseObject(raw) // a line that doesn't parse has no msg
		msg, _ := only(fields, "msg")
		if msg == "listening" {
			listening = true
			continue
		}
		if msg != enabledMsg {
			if bytes.Contains(raw, []byte(enabledMsg)) {
				return fmt.Errorf("gateway-bench log line %d mentions %q but can't be read as one", n, enabledMsg)
			}
			continue
		}
		enabled++
		name, _ := only(fields, "upstream")
		baseURL, err := only(fields, "base_url")
		if err != nil {
			return fmt.Errorf("gateway-bench upstream %q: %w", name, err)
		}
		if baseURL != mock {
			return fmt.Errorf("gateway-bench upstream %q is not on the mock (%s)", name, mock)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read gateway-bench log: %w", err)
	}
	if !listening {
		return errors.New(`gateway-bench did not log "listening"; see make logs`)
	}
	if enabled == 0 {
		return errors.New(`gateway-bench logged no "upstream enabled" line`)
	}
	return nil
}

// field is one key and its raw value in a JSON object.
type field struct {
	key string
	val json.RawMessage
}

// parseObject returns the top-level fields of the JSON object in raw, in
// order and with any duplicate keys kept, which decoding into a map or a
// struct would hide.
func parseObject(raw []byte) ([]field, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, errors.New("not a JSON object")
	}
	var fields []field
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, _ := tok.(string)
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil, err
		}
		fields = append(fields, field{key, val})
	}
	if _, err := dec.Token(); err != nil { // the closing brace
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("data after the JSON object")
	}
	return fields, nil
}

// only returns the string value of the one field whose key equals name
// ignoring case. It is an error if there is none, more than one, or the
// value is not a string.
func only(fields []field, name string) (string, error) {
	var vals []json.RawMessage
	for _, f := range fields {
		if strings.EqualFold(f.key, name) {
			vals = append(vals, f.val)
		}
	}
	if len(vals) != 1 {
		return "", fmt.Errorf("%d %s fields, want 1", len(vals), name)
	}
	var s string
	// null would decode to "" without an error.
	if !bytes.HasPrefix(bytes.TrimSpace(vals[0]), []byte(`"`)) || json.Unmarshal(vals[0], &s) != nil {
		return "", fmt.Errorf("%s is not a string", name)
	}
	return s, nil
}
