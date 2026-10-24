package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A subscribed writer can stall without retaining the producer's work lock.
type failureForwardWriter struct {
	http.ResponseWriter
	ctx     context.Context
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *failureForwardWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *failureForwardWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	select {
	case <-w.release:
		return w.ResponseWriter.Write(p)
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	}
}

func failureForwardEvent(t *testing.T, r *bufio.Reader) map[string]any {
	t.Helper()
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data: ") {
			var event map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data: "))), &event); err != nil {
				t.Fatal(err)
			}
			return event
		}
	}
}

func TestBackgroundForwardsFailedTerminals(t *testing.T) {
	terminals := []struct {
		kind string
		raw  string
	}{
		{"response.failed", `{"type":"response.failed","sequence_number":57,"future_field":{"values":[1,null]},"response":{"id":"resp_upstream","object":"response","status":"failed","store":true,"output":[],"error":{"code":"server_error","message":"503 original upstream failure","opaque":[true,null]},"future_response":{"kept":true}}}`},
		{"response.incomplete", `{"type":"response.incomplete","sequence_number":57,"future_field":{"values":[1,null]},"response":{"id":"resp_upstream","object":"response","status":"incomplete","store":true,"output":[],"error":null,"incomplete_details":{"reason":"max_output_tokens","opaque":[true,null]},"future_response":{"kept":true}}}`},
		{"error", `{"type":"error","sequence_number":57,"future_field":{"values":[1,null]},"response_id":"resp_upstream","code":"server_error","message":"503 original upstream failure","param":null,"error":{"code":"vendor_error","message":"original nested failure","opaque":[true,null]}}`},
		{"error-flat", `{"type":"error","sequence_number":57,"future_field":{"values":[1,null]},"response_id":"resp_upstream","code":"server_error","message":"503 original upstream failure","param":null}`},
		{"error-null-code-empty-message", `{"type":"error","sequence_number":57,"future_field":{"values":[1,null]},"response_id":"resp_upstream","code":null,"message":"","param":null}`},
		{"error-empty-code-empty-message", `{"type":"error","sequence_number":57,"future_field":{"values":[1,null]},"response_id":"resp_upstream","code":"","message":"","param":null}`},
		{"error-nested", `{"type":"error","sequence_number":57,"future_field":{"values":[1,null]},"response_id":"resp_upstream","error":{"code":"upstream_failure","message":"503 original upstream failure","opaque":[true,null]}}`},
	}
	for _, terminal := range terminals {
		for _, native := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/native=%t", terminal.kind, native), func(t *testing.T) {
				var creates atomic.Int32
				releaseUpstream, releaseSlow := make(chan struct{}), make(chan struct{})
				var upstreamOnce, slowOnce sync.Once
				defer upstreamOnce.Do(func() { close(releaseUpstream) })
				defer slowOnce.Do(func() { close(releaseSlow) })
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/v1/sub2api/billing" {
						_, _ = io.WriteString(w, `{}`)
						return
					}
					if creates.Add(1) != 1 {
						http.Error(w, "unexpected generation", http.StatusInternalServerError)
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_upstream\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}}\n\n")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"visible prefix\"}\n\n")
					w.(http.Flusher).Flush()
					select {
					case <-releaseUpstream:
					case <-r.Context().Done():
						return
					}
					_, _ = fmt.Fprintf(w, "data: %s\n\n", terminal.raw)
					// A failed generation must stop before an erroneous later success.
					_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_upstream\",\"status\":\"completed\",\"output\":[]}}\n\n")
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				}))
				t.Cleanup(up.Close)
				u, _ := url.Parse(up.URL)
				_, _, k := newTestKeeper(t, func(c *config) { c.upstream = u })
				blocked := make(chan struct{})
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("X-Fixture-Slow") != "" {
						w = &failureForwardWriter{ResponseWriter: w, ctx: r.Context(), entered: blocked, release: releaseSlow}
					}
					k.ServeHTTP(w, r)
				}))
				defer srv.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				body := `{"model":"m","input":"A","store":true,"background":true,"stream":true}`
				if native {
					body = `{"model":"m","input":"A","previous_response_id":"resp_unseen","store":true,"background":true,"stream":true}`
				}
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(body))
				req.Header.Set("Authorization", "Bearer owner")
				creator, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer creator.Body.Close()
				reader := bufio.NewReader(creator.Body)
				accepted := []map[string]any{failureForwardEvent(t, reader), failureForwardEvent(t, reader)}
				id := accepted[0]["response"].(map[string]any)["id"].(string)
				if native != (id == "resp_upstream") {
					t.Fatalf("wrong response alias: native=%t id=%q", native, id)
				}
				k.cacheMu.Lock()
				task := k.tasks[id]
				k.cacheMu.Unlock()
				var subscribers []*http.Response
				for i := range 2 {
					get, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/responses/"+id+"?stream=true&starting_after=0", nil)
					get.Header.Set("Authorization", "Bearer owner")
					if i == 1 {
						get.Header.Set("X-Fixture-Slow", "true")
					}
					response, err := http.DefaultClient.Do(get)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					if response.StatusCode != http.StatusOK {
						t.Fatalf("active subscriber status=%d", response.StatusCode)
					}
					subscribers = append(subscribers, response)
				}
				select {
				case <-blocked:
				case <-ctx.Done():
					t.Fatal("slow subscriber did not block")
				}
				upstreamOnce.Do(func() { close(releaseUpstream) })
				select {
				case <-task.finished:
				case <-ctx.Done():
					t.Fatal("slow subscriber blocked producer completion")
				}
				task.mu.Lock()
				complete, failed, retained := task.complete, task.err != nil, task.log != nil
				task.mu.Unlock()
				if complete || !failed || !retained {
					t.Fatalf("failed task state before active reader drains: complete=%t failed=%t retained=%t", complete, failed, retained)
				}
				slowOnce.Do(func() { close(releaseSlow) })
				collect := func(r io.Reader) []map[string]any {
					var events []map[string]any
					err := readSSE(r, nil, func(raw []byte) error {
						var event map[string]any
						if err := json.Unmarshal(raw, &event); err != nil {
							return err
						}
						events = append(events, event)
						return nil
					})
					if err != nil {
						t.Fatalf("original failure terminal was not forwarded cleanly: events=%v err=%v", events, err)
					}
					return events
				}
				streams := [][]map[string]any{append(accepted, collect(reader)...)}
				for _, subscriber := range subscribers {
					streams = append(streams, collect(subscriber.Body))
				}
				var want map[string]any
				_ = json.Unmarshal([]byte(terminal.raw), &want)
				want["sequence_number"] = float64(2)
				if response, ok := want["response"].(map[string]any); ok {
					response["id"], response["background"] = id, true
				}
				if _, ok := want["response_id"]; ok {
					want["response_id"] = id
				}
				for i, events := range streams {
					first := 0
					if i > 0 {
						first = 1
					}
					if len(events) != 3-first {
						t.Fatalf("subscriber %d terminal count: events=%v", i, events)
					}
					for n, event := range events {
						if event["sequence_number"] != float64(n+first) || event["type"] == "response.completed" {
							t.Fatalf("subscriber %d invalid event sequence: %v", i, events)
						}
					}
					if !reflect.DeepEqual(events[len(events)-1], want) {
						t.Fatalf("subscriber %d original terminal changed:\n got=%v\nwant=%v", i, events[len(events)-1], want)
					}
				}
				if status, body := gateGet(t, srv, id, "?stream=true&starting_after=0"); status != http.StatusConflict || !strings.Contains(body, "response_replay_unavailable") {
					t.Fatalf("failed task became replayable: %d %s", status, body)
				}
				if status, body := gateGet(t, srv, id, ""); status != http.StatusOK || !strings.Contains(body, `"status":"failed"`) || strings.Contains(body, "original upstream failure") {
					t.Fatalf("failed snapshot became an answer: %d %s", status, body)
				}
				if !native {
					status, body, _ := post(t, srv, "Bearer owner", `{"model":"m","input":"B","previous_response_id":"`+id+`"}`)
					if status != http.StatusBadRequest || !strings.Contains(body, "previous_response_not_found") {
						t.Fatalf("failed checkpoint retained: %d %s", status, body)
					}
				}
				for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(time.Millisecond) {
					k.cacheMu.Lock()
					bytes := k.eventBytes
					k.cacheMu.Unlock()
					if bytes == 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("failed event log retained after readers drained: %d", bytes)
					}
				}
				if entries, _ := k.store.stats(); entries != 0 || creates.Load() != 1 {
					t.Fatalf("failed generation became cached/retried: entries=%d upstream POST=%d", entries, creates.Load())
				}
			})
		}
	}
}

func TestBackgroundFailureTerminalValidation(t *testing.T) {
	cases := []string{
		`{"type":"response.failed"}`,
		`{"type":"response.incomplete"}`,
		`{"type":"response.failed","response":null}`,
		`{"type":"response.failed","response":{"id":"resp_upstream","status":"completed","output":[]}}`,
		`{"type":"response.failed","response":{"status":"failed","output":[]}}`,
		`{"type":"response.failed","response":{"id":"resp_other","status":"failed","output":[]}}`,
		`{"type":"response.failed","response":{"id":7,"status":"failed","output":[]}}`,
		`{"type":"response.failed","response":{"id":"resp_upstream","status":"failed","output":"bad"}}`,
		`{"type":"response.incomplete","response":{"id":"resp_upstream","status":"failed","output":[]}}`,
		`{"type":"error","response_id":"resp_other","message":"failure"}`,
		`{"type":"error","response_id":7,"message":"failure"}`,
		`{"type":"error","response":null,"message":"failure"}`,
		`{"type":"error"}`,
		`{"type":"error","error":null}`,
		`{"type":"error","error":{"code":7}}`,
		`{"type":"error","message":7}`,
		`{"type":"error","code":"server_error"}`,
		`{"type":"error","code":"server_error","message":null}`,
		`{"type":"error","response":{"id":"resp_other","status":"failed","output":[]},"message":"failure"}`,
		`{"type":"error","response":{"status":"failed"},"message":"failure"}`,
	}
	_, _, k := newTestKeeper(t)
	for i, raw := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			log, err := k.newSpool()
			if err != nil {
				t.Fatal(err)
			}
			task := &responseTask{keeper: k, id: "resp_ck_validation", upstreamID: "resp_upstream", log: log,
				changed: make(chan struct{}), ready: make(chan struct{})}
			defer task.discard()
			if err := task.record(context.Background(), []byte(raw), nil); err == nil || err == errBackgroundFailure {
				t.Fatalf("invalid terminal accepted: %s error=%v", raw, err)
			}
			if task.count != 0 || task.log.size != 0 || task.complete || task.failureTerminal || task.err != nil {
				t.Fatalf("invalid terminal changed task state: %s", raw)
			}
		})
	}
}

func TestBackgroundFailureTerminalAcceptsPublicUpstreamID(t *testing.T) {
	_, _, k := newTestKeeper(t)
	log, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	task := &responseTask{keeper: k, id: "resp_ck_public", upstreamID: "msg_vendor", log: log,
		changed: make(chan struct{}), ready: make(chan struct{})}
	defer task.discard()
	raw := `{"type":"response.failed","response":{"id":"resp_msg_vendor","status":"failed","output":[]}}`
	if err := task.record(context.Background(), []byte(raw), nil); err != errBackgroundFailure {
		t.Fatalf("existing upstream public-id rewrite rejected: %v", err)
	}
	if !task.failureTerminal || task.complete || task.count != 1 {
		t.Fatalf("failure became success or was not recorded: %+v", task)
	}
}
