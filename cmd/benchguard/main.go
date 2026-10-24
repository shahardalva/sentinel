// Command benchguard reads a gateway's JSON log on stdin and fails unless
// every upstream the gateway enabled points at the mock. make bench-gateway
// runs it on gateway-bench's startup log before k6 starts, so a load test
// never reaches a real API.
//
//	docker logs <container> 2>&1 | go run ./cmd/benchguard -mock https://mockupstream:9090
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	mock := flag.String("mock", "", "the mock's base URL, such as https://mockupstream:9090")
	flag.Parse()
	if err := check(os.Stdin, *mock); err != nil {
		fmt.Fprintf(os.Stderr, "bench-gateway: %v; refusing to run\n", err)
		os.Exit(1)
	}
}

// logLine holds the fields of a gateway log line that check reads.
type logLine struct {
	Msg      string  `json:"msg"`
	Upstream string  `json:"upstream"`
	BaseURL  *string `json:"base_url"`
}

// check reads gateway log lines from r. It returns nil only if the gateway
// logged "listening", logged at least one "upstream enabled" line, and every
// such line's base_url is mock or starts with mock followed by "/". Lines
// that are not JSON are skipped. Errors name the upstream at fault but never
// its base URL.
func check(r io.Reader, mock string) error {
	if mock == "" {
		return errors.New("benchguard needs -mock")
	}
	var listening bool
	var enabled int
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var line logLine
		if json.Unmarshal(sc.Bytes(), &line) != nil {
			continue
		}
		switch line.Msg {
		case "listening":
			listening = true
		case "upstream enabled":
			enabled++
			if line.BaseURL == nil || !onMock(*line.BaseURL, mock) {
				return fmt.Errorf("gateway-bench upstream %q is not on the mock (%s)", line.Upstream, mock)
			}
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

// onMock reports whether baseURL is mock itself or a path under it. A bare
// prefix match would accept https://mockupstream:9090.example.com.
func onMock(baseURL, mock string) bool {
	return baseURL == mock || strings.HasPrefix(baseURL, mock+"/")
}
