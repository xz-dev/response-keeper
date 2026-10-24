package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const keeperResponsePrefix = "resp_ck_"

var errBackgroundFailure = errors.New("upstream response ended unsuccessfully")

type backgroundTaskKey struct{}

// Event payloads live in the existing budgeted disk spool, not per-reader RAM.
// Length-prefixed records allow cursor seeking with constant-size metadata.
type responseTask struct {
	keeper                             *keeper
	id                                 string
	upstreamID                         string
	owner                              [32]byte
	cancel                             context.CancelFunc
	mu                                 sync.Mutex
	log                                *spool
	header                             http.Header
	status                             int
	stream                             bool
	nativeParent                       bool
	count                              int
	final                              int64
	done, complete, cancelled, expired bool
	failureTerminal                    bool
	err                                error
	readers, pending                   int // attached readers; reads awaiting validation
	changed                            chan struct{}
	ready                              chan struct{}
	readyOnce                          sync.Once
	finished                           chan struct{}
	timer                              *time.Timer
}

func (t *responseTask) signal() {
	close(t.changed)
	t.changed = make(chan struct{})
}

func (k *keeper) startBackground(w http.ResponseWriter, r *http.Request) {
	if err := r.Context().Err(); err != nil {
		return
	}
	body, err := r.Body.(*spoolBody).spool.requestBody()
	if err != nil {
		writeAPIError(w, workingError(err))
		return
	}
	log, err := k.newSpool()
	if err != nil {
		body.Close()
		writeAPIError(w, workingError(err))
		return
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	t := &responseTask{keeper: k, id: keeperResponsePrefix + rand.Text(), owner: k.ownerOf(r), cancel: cancel,
		log: log, readers: 1, changed: make(chan struct{}), ready: make(chan struct{}), finished: make(chan struct{})}
	k.cacheMu.Lock()
	if len(k.tasks) >= maxTurns {
		k.cacheMu.Unlock()
		body.Close()
		log.Close()
		cancel()
		writeAPIError(w, workingError(errors.New("task limit")))
		return
	}
	k.tasks[t.id] = t
	k.cacheMu.Unlock()
	req := r.Clone(context.WithValue(ctx, backgroundTaskKey{}, t))
	req.Body = body
	stripUntrustedForwarding(req.Header)
	go func() {
		defer close(t.finished)
		defer body.Close()
		defer cancel()
		writer := &taskWriter{task: t, header: make(http.Header)}
		k.serveHTTP(writer, req)
		t.mu.Lock()
		t.done = true
		if t.stream && !t.complete && t.err == nil {
			t.err = io.ErrUnexpectedEOF
		}
		if t.timer != nil {
			t.timer.Stop()
		}
		if !t.expired {
			t.timer = time.AfterFunc(k.cfg.ttl, t.expire)
		}
		t.readyOnce.Do(func() { close(t.ready) })
		t.signal()
		discard := t.err != nil || t.cancelled
		abandonedHTTP := !t.stream && t.readers == 0
		// Attached readers (including a creator not yet following) still get the
		// recorded prefix; the last one to leave discards the failed log.
		attached := t.readers > 0
		t.mu.Unlock()
		if abandonedHTTP {
			t.expire()
		} else if discard {
			if !t.nativeParent {
				k.cacheMu.Lock()
				if err := k.store.forget(t.id); err != nil {
					logNotRetained(err)
				}
				k.cacheMu.Unlock()
			}
			if !attached {
				t.discard()
			}
		}
	}()
	defer t.unsubscribe()
	select {
	case <-r.Context().Done():
		return
	case <-t.ready:
	}
	t.mu.Lock()
	copyHeader(w.Header(), t.header)
	status := t.status
	if status == 0 {
		status = http.StatusBadGateway
	}
	stream := t.stream
	t.mu.Unlock()
	w.Header().Del("Content-Length")
	w.WriteHeader(status)
	if err := t.follow(w, r, -1, stream); err != nil && r.Context().Err() == nil {
		panic(http.ErrAbortHandler)
	}
	if !stream {
		t.expire()
	}
}

func stripUntrustedForwarding(header http.Header) {
	for _, name := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-IP"} {
		header.Del(name)
	}
}

func (t *responseTask) unsubscribe() {
	t.mu.Lock()
	t.readers--
	abandonedHTTP := t.readers == 0 && t.done && !t.stream
	// The generation finished as failed/cancelled while readers drained its prefix.
	failedDrained := t.readers == 0 && t.done && t.stream && (t.err != nil || t.cancelled)
	if t.readers == 0 && !t.done && !t.expired {
		if t.status == 0 || !t.stream {
			t.cancel()
		} else {
			t.timer = time.AfterFunc(t.keeper.cfg.ttl, t.expire)
		}
	}
	t.mu.Unlock()
	if abandonedHTTP {
		t.expire()
	} else if failedDrained {
		t.discard()
	}
}

func (t *responseTask) discard() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.log == nil {
		return
	}
	t.keeper.cacheMu.Lock()
	if err := t.log.Close(); err == nil {
		t.keeper.eventBytes -= t.log.size
	}
	t.keeper.cacheMu.Unlock()
	t.log = nil
}

func (t *responseTask) expire() {
	t.mu.Lock()
	t.expired = true
	if t.timer != nil {
		t.timer.Stop()
	}
	t.cancel()
	t.signal()
	t.mu.Unlock()
	t.keeper.cacheMu.Lock()
	delete(t.keeper.tasks, t.id)
	t.keeper.cacheMu.Unlock()
	t.discard()
}

// Caller holds task.mu; both retained histories and event files share cacheMu.
func (t *responseTask) append(p []byte) error {
	if t.log == nil || t.expired || t.err != nil {
		return io.ErrClosedPipe
	}
	if t.log.size+int64(len(p)) > maxResponseBytes {
		return errWorkSize
	}
	k := t.keeper
	k.cacheMu.Lock()
	defer k.cacheMu.Unlock()
	_, history := k.store.stats()
	if k.cfg.maxStoreBytes > 0 && history+k.eventBytes+int64(len(p)) > k.cfg.maxStoreBytes {
		return errWorkSize
	}
	n, err := t.log.Write(p)
	k.eventBytes += int64(n)
	return err
}

// parsed, when non-nil, is raw's already decoded top level (see responseFrameParsed).
func (t *responseTask) record(ctx context.Context, raw []byte, parsed map[string]json.RawMessage) error {
	leave, err := t.keeper.work.enter(ctx)
	if err != nil {
		return err
	}
	defer leave()
	scratch := responseScratch(len(raw))
	if !t.keeper.work.reserveOutput(scratch) {
		return errWorkSize
	}
	defer t.keeper.work.freeOutput(scratch)
	event := parsed
	if event == nil {
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
	}
	var kind string
	if err := json.Unmarshal(event["type"], &kind); err != nil {
		return err
	}
	failed := kind == "error" || kind == "response.failed" || kind == "response.incomplete"
	if kind == "error" {
		failure := event
		if event["code"] == nil && event["message"] == nil {
			failure, err = responseObject(event["error"])
			if err != nil {
				return err
			}
		}
		_, codeErr := responseString(failure["code"])
		var message *string
		messageErr := json.Unmarshal(failure["message"], &message)
		if codeErr != nil || messageErr != nil || message == nil {
			return errUpstreamData
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.expired || t.cancelled || t.err != nil {
		return io.ErrClosedPipe
	}
	if t.complete {
		return errUpstreamData
	}
	if failed && event["response_id"] != nil {
		id, err := responseString(event["response_id"])
		if err != nil || id == "" || (t.upstreamID != "" && id != t.upstreamID && id != publicID(t.upstreamID)) {
			return errUpstreamData
		}
	}
	event["sequence_number"] = json.RawMessage(strconv.Itoa(t.count))
	if response := event["response"]; response != nil {
		var doc map[string]json.RawMessage
		if err := json.Unmarshal(response, &doc); err != nil {
			return err
		}
		if doc == nil {
			return errUpstreamData
		}
		if failed {
			id, idErr := responseString(doc["id"])
			status, statusErr := responseString(doc["status"])
			if idErr != nil || statusErr != nil || id == "" || (kind != "error" && "response."+status != kind) ||
				(t.upstreamID != "" && id != t.upstreamID && id != publicID(t.upstreamID)) {
				return errUpstreamData
			}
			if _, err := responseItems(doc["output"]); err != nil {
				return err
			}
		}
		if t.nativeParent && t.count == 0 {
			var upstreamID string
			if err := json.Unmarshal(doc["id"], &upstreamID); err != nil || upstreamID == "" || strings.Contains(upstreamID, "/") {
				return errUpstreamData
			}
			t.keeper.cacheMu.Lock()
			if other := t.keeper.tasks[upstreamID]; other != nil && other != t {
				t.keeper.cacheMu.Unlock()
				return errStoreOwner
			}
			delete(t.keeper.tasks, t.id)
			t.id = upstreamID
			t.keeper.tasks[t.id] = t
			t.keeper.cacheMu.Unlock()
		}
		doc["id"], _ = json.Marshal(t.id)
		doc["background"] = json.RawMessage("true")
		event["response"], err = json.Marshal(doc)
		if err != nil {
			return err
		}
	}
	if failed && event["response"] == nil && kind != "error" {
		return errUpstreamData
	}
	if _, ok := event["response_id"]; ok {
		event["response_id"], _ = json.Marshal(t.id)
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	wire := []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", kind, data))
	if len(wire) > maxEventBytes {
		return errWorkSize
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(wire)))
	offset := t.log.size
	if err := t.append(append(length[:], wire...)); err != nil {
		return err
	}
	t.count++
	if kind == "response.completed" {
		t.complete = true
		t.final = offset
	}
	if failed {
		// Attached readers drain this bounded log; new reads fail and the last reader discards it.
		t.failureTerminal, t.err = true, errBackgroundFailure
	}
	t.readyOnce.Do(func() { close(t.ready) })
	t.signal()
	if failed {
		return errBackgroundFailure
	}
	return nil
}

type taskWriter struct {
	task   *responseTask
	header http.Header
}

func (w *taskWriter) Header() http.Header { return w.header }
func (w *taskWriter) WriteHeader(status int) {
	t := w.task
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.status != 0 {
		return
	}
	t.status, t.header = status, w.header.Clone()
	t.stream = status == http.StatusOK && strings.Contains(t.header.Get("Content-Type"), "text/event-stream")
	if !t.stream {
		t.readyOnce.Do(func() { close(t.ready) })
	}
	t.signal()
}
func (w *taskWriter) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	t := w.task
	t.mu.Lock()
	defer t.mu.Unlock()
	// Valid SSE events go through record; this path is only raw HTTP or an
	// existing relay failure. Failed prefixes must never become replayable.
	if t.stream {
		t.err = io.ErrUnexpectedEOF
		return 0, t.err
	}
	if err := t.append(p); err != nil {
		t.err = err
		return 0, err
	}
	t.signal()
	return len(p), nil
}
func (w *taskWriter) Flush() {}

func (t *responseTask) follow(w http.ResponseWriter, r *http.Request, after int, framed bool) error {
	t.mu.Lock()
	if t.log == nil {
		t.mu.Unlock()
		return io.ErrUnexpectedEOF
	}
	body, err := t.log.requestBody()
	t.mu.Unlock()
	if err != nil {
		return err
	}
	defer body.Close()
	var offset int64
	sequence := 0
	controller := http.NewResponseController(w)
	defer controller.SetWriteDeadline(time.Time{})
	for {
		t.mu.Lock()
		if t.cancelled || t.expired {
			t.mu.Unlock()
			return io.ErrUnexpectedEOF
		}
		// Failed generations drain their prefix and original terminal; truncated
		// streams still abort once their recorded prefix ends.
		size, done, failed, changed := body.spool.size, t.done, t.err != nil, t.changed
		terminal := t.failureTerminal
		t.mu.Unlock()
		if offset < size {
			n := size - offset
			if framed {
				var length [8]byte
				if _, err := body.spool.ReadAt(length[:], offset); err != nil {
					return err
				}
				n = int64(binary.BigEndian.Uint64(length[:]))
				offset += 8
				if n <= 0 || n > maxEventBytes || offset+n > size {
					return errUpstreamData
				}
			}
			if !framed || sequence > after {
				_ = controller.SetWriteDeadline(time.Now().Add(workWait))
				if _, err := io.CopyN(w, io.NewSectionReader(body.spool.File, body.spool.offset+offset, n), n); err != nil {
					return err
				}
				if err := controller.Flush(); err != nil {
					return err
				}
			}
			offset += n
			sequence++
			continue
		}
		if failed {
			// A forwarded failure is a complete SSE terminal, not a truncated transport.
			if terminal {
				return nil
			}
			return io.ErrUnexpectedEOF
		}
		if done {
			return nil
		}
		select {
		case <-r.Context().Done():
			return r.Context().Err()
		case <-changed:
		}
	}
}

func (k *keeper) taskOperation(w http.ResponseWriter, r *http.Request) bool {
	path := strings.TrimPrefix(r.URL.Path, "/v1")
	if !strings.HasPrefix(path, "/responses/") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(path, "/responses/"), "/")
	if len(parts) != 1 && !(len(parts) == 2 && parts[1] == "cancel") {
		return false
	}
	if (len(parts) == 1 && r.Method != http.MethodGet) || (len(parts) == 2 && r.Method != http.MethodPost) {
		return false
	}
	if !strings.HasPrefix(parts[0], keeperResponsePrefix) {
		k.cacheMu.Lock()
		_, cached := k.tasks[parts[0]]
		k.cacheMu.Unlock()
		if !cached {
			return false
		}
	}
	// One task's reconnect storm cannot hold the shared socket pool that other
	// tasks need to reattach (storm and recovery reads are indistinguishable).
	// Reads are counted per task before validation takes a shared slot; cancel
	// is never refused by this per-task bound.
	var admitted *responseTask // holds one pending count until it becomes a reader
	if len(parts) == 1 {
		k.cacheMu.Lock()
		pending := k.tasks[parts[0]]
		k.cacheMu.Unlock()
		// Only the owner's credential spends this task's budget, so unauthenticated
		// or foreign callers cannot target it (digest isolates, billing still decides).
		if pending != nil && pending.owner == k.ownerOf(r) {
			pending.mu.Lock()
			full := pending.readers+pending.pending >= maxTaskReaders
			if !full {
				pending.pending++
				admitted = pending
			}
			pending.mu.Unlock()
			if full {
				writeAPIError(w, errNativeSocketLimit)
				return true
			}
			defer func() {
				if admitted != nil {
					admitted.mu.Lock()
					admitted.pending--
					admitted.mu.Unlock()
				}
			}()
		}
	}
	// Local reads and authentication work share the existing socket ceiling.
	if !trySlot(k.work.sockets) {
		writeAPIError(w, errNativeSocketLimit)
		return true
	}
	defer func() { <-k.work.sockets }()
	// Authentication is revalidated for every local operation; hashes only
	// isolate owners and are never used as proof that a key remains valid.
	target := *k.cfg.upstream
	target.Path = strings.TrimRight(target.Path, "/") + "/v1/sub2api/billing"
	// Preserve credential inputs so upstream query-key rejection still applies.
	target.RawQuery = r.URL.RawQuery
	auth, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target.String(), nil)
	if err != nil {
		writeAPIError(w, workingError(err))
		return true
	}
	copyHeader(auth.Header, r.Header)
	stripUntrustedForwarding(auth.Header)
	check, err := k.client.Do(auth)
	if err != nil {
		writeAPIError(w, workingError(err))
		return true
	}
	if check.StatusCode < 200 || check.StatusCode >= 300 {
		copyHeader(w.Header(), check.Header)
		w.WriteHeader(check.StatusCode)
		_, _ = io.Copy(w, check.Body)
		check.Body.Close()
		return true
	}
	check.Body.Close()
	k.cacheMu.Lock()
	t := k.tasks[parts[0]]
	k.cacheMu.Unlock()
	if t == nil || t.owner != k.ownerOf(r) {
		writeAPIError(w, &apiError{status: 404, typ: "invalid_request_error", code: "response_not_found", message: "Response not found."})
		return true
	}
	// Query() silently drops malformed fields, which would hide a present cursor.
	query, queryErr := url.ParseQuery(r.URL.RawQuery)
	if queryErr != nil && len(parts) == 1 {
		writeAPIError(w, &apiError{status: 400, typ: "invalid_request_error", message: "Invalid query."})
		return true
	}
	t.mu.Lock()
	if len(parts) == 2 && !t.done {
		t.cancelled = true
		t.cancel()
		t.signal()
	}
	if t.expired || !t.stream {
		t.mu.Unlock()
		writeAPIError(w, &apiError{status: 404, typ: "invalid_request_error", message: "Response not found."})
		return true
	}
	stream := query.Get("stream") == "true" && len(parts) == 1
	if stream && (t.err != nil || t.cancelled) {
		t.mu.Unlock()
		writeAPIError(w, &apiError{status: 409, typ: "invalid_request_error", code: "response_replay_unavailable", message: "Response cannot be replayed."})
		return true
	}
	after := -1
	if stream && query.Has("starting_after") {
		var err error
		// A present cursor must name an event already recorded; empty is invalid.
		if after, err = strconv.Atoi(query.Get("starting_after")); err != nil || after < 0 || after >= t.count {
			t.mu.Unlock()
			writeAPIError(w, &apiError{status: 400, typ: "invalid_request_error", message: "Invalid starting_after."})
			return true
		}
	}
	// The pending count above already admitted this read; recheck attached
	// readers. Cancel was applied above; its short final reply is not refused.
	if len(parts) == 1 && t.readers >= maxTaskReaders {
		t.mu.Unlock()
		writeAPIError(w, errNativeSocketLimit)
		return true
	}
	if admitted == t {
		t.pending-- // converted to an attached reader under the same lock
		admitted = nil
	}
	t.readers++
	if !t.done && t.timer != nil {
		t.timer.Stop()
	}
	t.mu.Unlock()
	defer t.unsubscribe()
	if stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
		if err := t.follow(w, r, after, true); err != nil && r.Context().Err() == nil {
			panic(http.ErrAbortHandler)
		}
		return true
	}
	// Final response materialization is short and shares the existing work gate.
	leave, err := k.work.enter(r.Context())
	if err != nil {
		writeAPIError(w, workingError(err))
		return true
	}
	defer leave()
	t.mu.Lock()
	defer t.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if t.complete && t.log != nil {
		var length [8]byte
		_, err := t.log.ReadAt(length[:], t.final)
		n := int64(binary.BigEndian.Uint64(length[:]))
		if err != nil || n <= 0 || n > maxEventBytes {
			writeAPIError(w, responseError(errUpstreamData))
			return true
		}
		var response json.RawMessage
		err = readSSE(io.NewSectionReader(t.log.File, t.log.offset+t.final+8, n), nil, func(data []byte) error {
			var event struct {
				Response json.RawMessage `json:"response"`
			}
			if err := json.Unmarshal(data, &event); err != nil {
				return err
			}
			response = event.Response
			return nil
		}, k.work)
		if err != nil {
			writeAPIError(w, responseError(err))
			return true
		}
		_, _ = w.Write(response)
		return true
	}
	status := "in_progress"
	if t.cancelled {
		status = "cancelled"
	} else if t.err != nil {
		status = "failed"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"id": t.id, "object": "response", "background": true, "status": status, "output": []any{}})
	return true
}
