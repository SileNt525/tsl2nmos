package nmos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RegisterRoutes mounts all AMWA NMOS IS-04, IS-05, and IS-07 HTTP & WebSocket endpoints onto mux.
func RegisterRoutes(mux *http.ServeMux, store *Store, wsHub *WSHub) {
	mux.Handle("/x-nmos", withNMOSCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleNMOSRoot(w, r, store, wsHub)
	})))
	mux.Handle("/x-nmos/", withNMOSCORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleNMOSRoot(w, r, store, wsHub)
	})))
}

func withNMOSCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, PUT, POST, HEAD, OPTIONS, DELETE, PATCH")
		h.Set("Access-Control-Allow-Headers", "Content-Type, Accept, Authorization")
		h.Set("Access-Control-Max-Age", "3600")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func handleNMOSRoot(w http.ResponseWriter, r *http.Request, store *Store, wsHub *WSHub) {
	path := strings.TrimPrefix(r.URL.Path, "/x-nmos")
	path = strings.Trim(path, "/")
	if path == "" {
		writeJSON(w, http.StatusOK, []string{"node/", "connection/", "events/"})
		return
	}

	parts := strings.Split(path, "/")
	switch parts[0] {
	case "node":
		handleIS04Node(w, r, parts[1:], store)
	case "connection":
		handleIS05Connection(w, r, parts[1:], store)
	case "events":
		handleIS07Events(w, r, parts[1:], store, wsHub)
	default:
		writeNMOSError(w, http.StatusNotFound, "unknown NMOS API namespace")
	}
}

// --- IS-04 Node API (/x-nmos/node/{v1.2|v1.3}/...) ---

func handleIS04Node(w http.ResponseWriter, r *http.Request, parts []string, store *Store) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeNMOSError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if len(parts) == 0 {
		writeJSON(w, http.StatusOK, []string{"v1.2/", "v1.3/"})
		return
	}

	ver := parts[0]
	if ver != "v1.2" && ver != "v1.3" {
		writeNMOSError(w, http.StatusNotFound, "unsupported IS-04 version")
		return
	}

	if len(parts) == 1 {
		writeJSON(w, http.StatusOK, []string{
			"self/",
			"sources/",
			"flows/",
			"devices/",
			"senders/",
			"receivers/",
		})
		return
	}

	node, device, sources, flows, senders := store.SnapshotResources()
	resourceType := parts[1]
	resourceID := ""
	if len(parts) >= 3 {
		resourceID = parts[2]
	}
	if len(parts) > 3 {
		writeNMOSError(w, http.StatusNotFound, "not found")
		return
	}

	switch resourceType {
	case "self":
		if resourceID != "" {
			writeNMOSError(w, http.StatusNotFound, "not found")
			return
		}
		writeJSON(w, http.StatusOK, node)

	case "devices":
		if resourceID == "" {
			writeJSON(w, http.StatusOK, []DeviceResource{device})
			return
		}
		if resourceID == device.ID {
			writeJSON(w, http.StatusOK, device)
			return
		}
		writeNMOSError(w, http.StatusNotFound, "device not found")

	case "sources":
		if resourceID == "" {
			writeJSON(w, http.StatusOK, sources)
			return
		}
		for _, s := range sources {
			if s.ID == resourceID {
				writeJSON(w, http.StatusOK, s)
				return
			}
		}
		writeNMOSError(w, http.StatusNotFound, "source not found")

	case "flows":
		if resourceID == "" {
			writeJSON(w, http.StatusOK, flows)
			return
		}
		for _, f := range flows {
			if f.ID == resourceID {
				writeJSON(w, http.StatusOK, f)
				return
			}
		}
		writeNMOSError(w, http.StatusNotFound, "flow not found")

	case "senders":
		if resourceID == "" {
			writeJSON(w, http.StatusOK, senders)
			return
		}
		for _, s := range senders {
			if s.ID == resourceID {
				writeJSON(w, http.StatusOK, s)
				return
			}
		}
		writeNMOSError(w, http.StatusNotFound, "sender not found")

	case "receivers":
		if resourceID == "" {
			writeJSON(w, http.StatusOK, []any{})
			return
		}
		writeNMOSError(w, http.StatusNotFound, "receiver not found")

	default:
		writeNMOSError(w, http.StatusNotFound, "not found")
	}
}

// --- IS-05 Connection Management API (/x-nmos/connection/{v1.0|v1.1}/...) ---

func handleIS05Connection(w http.ResponseWriter, r *http.Request, parts []string, store *Store) {
	if len(parts) == 0 {
		writeJSON(w, http.StatusOK, []string{"v1.0/", "v1.1/"})
		return
	}

	ver := parts[0]
	if ver != "v1.0" && ver != "v1.1" {
		writeNMOSError(w, http.StatusNotFound, "unsupported IS-05 version")
		return
	}

	if len(parts) == 1 {
		writeJSON(w, http.StatusOK, []string{"bulk/", "single/"})
		return
	}

	switch parts[1] {
	case "single":
		handleIS05Single(w, r, ver, parts[2:], store)
	case "bulk":
		handleIS05Bulk(w, r, parts[2:], store)
	default:
		writeNMOSError(w, http.StatusNotFound, "not found")
	}
}

func handleIS05Single(w http.ResponseWriter, r *http.Request, ver string, parts []string, store *Store) {
	if len(parts) == 0 {
		writeJSON(w, http.StatusOK, []string{"senders/", "receivers/"})
		return
	}

	switch parts[0] {
	case "receivers":
		if len(parts) == 1 && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			writeJSON(w, http.StatusOK, []string{})
			return
		}
		writeNMOSError(w, http.StatusNotFound, "receiver not found")

	case "senders":
		if len(parts) == 1 {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				writeNMOSError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			_, _, _, _, senders := store.SnapshotResources()
			ids := make([]string, 0, len(senders))
			for _, s := range senders {
				ids = append(ids, s.ID+"/")
			}
			writeJSON(w, http.StatusOK, ids)
			return
		}

		senderID := parts[1]
		if len(parts) == 2 {
			if !store.hasSender(senderID) {
				writeNMOSError(w, http.StatusNotFound, "sender not found")
				return
			}
			subRoutes := []string{"constraints/", "staged/", "active/", "transportfile/"}
			if ver == "v1.1" {
				subRoutes = append(subRoutes, "transporttype/")
			}
			writeJSON(w, http.StatusOK, subRoutes)
			return
		}

		if len(parts) > 3 {
			writeNMOSError(w, http.StatusNotFound, "not found")
			return
		}

		leaf := parts[2]
		switch leaf {
		case "constraints":
			c, ok := store.getSenderConstraints(senderID)
			if !ok {
				writeNMOSError(w, http.StatusNotFound, "sender not found")
				return
			}
			writeJSON(w, http.StatusOK, c)

		case "staged":
			switch r.Method {
			case http.MethodGet, http.MethodHead:
				st, ok := store.getSenderStaged(senderID)
				if !ok {
					writeNMOSError(w, http.StatusNotFound, "sender not found")
					return
				}
				writeJSON(w, http.StatusOK, st)
			case http.MethodPatch:
				body, err := io.ReadAll(io.LimitReader(r.Body, 65536))
				if err != nil {
					writeNMOSError(w, http.StatusBadRequest, "failed to read body")
					return
				}
				resp, code, errMsg := store.patchSenderStaged(senderID, body)
				if code != http.StatusOK && code != http.StatusAccepted {
					writeNMOSError(w, code, errMsg)
					return
				}
				writeJSON(w, code, resp)
			default:
				writeNMOSError(w, http.StatusMethodNotAllowed, "method not allowed")
			}

		case "active":
			act, ok := store.getSenderActive(senderID)
			if !ok {
				writeNMOSError(w, http.StatusNotFound, "sender not found")
				return
			}
			writeJSON(w, http.StatusOK, act)

		case "transportfile":
			if !store.hasSender(senderID) {
				writeNMOSError(w, http.StatusNotFound, "sender not found")
				return
			}
			// Per AMWA IS-07 spec, WebSocket senders do not use SDP transport files (404 Not Found).
			writeNMOSError(w, http.StatusNotFound, "WebSocket transport does not use SDP transportfile")

		case "transporttype":
			if ver != "v1.1" || !store.hasSender(senderID) {
				writeNMOSError(w, http.StatusNotFound, "not found")
				return
			}
			writeJSON(w, http.StatusOK, TransportWebsocket)

		default:
			writeNMOSError(w, http.StatusNotFound, "not found")
		}

	default:
		writeNMOSError(w, http.StatusNotFound, "not found")
	}
}

func handleIS05Bulk(w http.ResponseWriter, r *http.Request, parts []string, store *Store) {
	if len(parts) == 0 {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			writeJSON(w, http.StatusOK, []string{"senders/", "receivers/"})
			return
		}
		writeNMOSError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	switch parts[0] {
	case "receivers":
		if r.Method == http.MethodGet {
			writeNMOSError(w, http.StatusMethodNotAllowed, "GET not supported on bulk endpoint")
			return
		}
		if r.Method == http.MethodPost {
			writeJSON(w, http.StatusOK, []BulkResponseItem{})
			return
		}
		writeNMOSError(w, http.StatusMethodNotAllowed, "method not allowed")

	case "senders":
		if r.Method != http.MethodPost {
			writeNMOSError(w, http.StatusMethodNotAllowed, "only POST is supported on bulk/senders")
			return
		}
		var reqs []BulkSenderItem
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&reqs); err != nil {
			writeNMOSError(w, http.StatusBadRequest, "invalid JSON array: "+err.Error())
			return
		}
		results := make([]BulkResponseItem, 0, len(reqs))
		for _, item := range reqs {
			rawParams, _ := json.Marshal(item.Params)
			_, code, errMsg := store.patchSenderStaged(item.ID, rawParams)
			results = append(results, BulkResponseItem{
				ID:    item.ID,
				Code:  code,
				Error: errMsg,
			})
		}
		writeJSON(w, http.StatusOK, results)

	default:
		writeNMOSError(w, http.StatusNotFound, "not found")
	}
}

// --- IS-07 Event & Tally API (/x-nmos/events/v1.0/...) ---

func handleIS07Events(w http.ResponseWriter, r *http.Request, parts []string, store *Store, wsHub *WSHub) {
	if len(parts) == 0 {
		writeJSON(w, http.StatusOK, []string{"v1.0/"})
		return
	}
	if parts[0] != "v1.0" {
		writeNMOSError(w, http.StatusNotFound, "unsupported IS-07 version")
		return
	}
	if len(parts) == 1 {
		writeJSON(w, http.StatusOK, []string{"sources/"})
		return
	}

	switch parts[1] {
	case "ws":
		// Primary WebSocket transport endpoint advertised in IS-05 connection_uri.
		wsHub.ServeHTTP(w, r)
		return

	case "devices":
		// Also accept WebSocket upgrade on /x-nmos/events/v1.0/devices/{deviceId} for broad interoperability.
		wsHub.ServeHTTP(w, r)
		return

	case "sources":
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			writeNMOSError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if len(parts) == 2 {
			_, _, sources, _, _ := store.SnapshotResources()
			ids := make([]string, 0, len(sources))
			for _, s := range sources {
				ids = append(ids, s.ID+"/")
			}
			writeJSON(w, http.StatusOK, ids)
			return
		}

		sourceID := parts[2]
		msg, _, ok := store.GetStateMessageBySourceID(sourceID)
		if !ok {
			writeNMOSError(w, http.StatusNotFound, "event source not found")
			return
		}

		if len(parts) == 3 {
			writeJSON(w, http.StatusOK, []string{"state/", "type/"})
			return
		}
		if len(parts) > 4 {
			writeNMOSError(w, http.StatusNotFound, "not found")
			return
		}

		switch parts[3] {
		case "type":
			writeJSON(w, http.StatusOK, EventTypeBooleanSchema{Type: EventTypeBoolean})
		case "state":
			writeJSON(w, http.StatusOK, msg)
		default:
			writeNMOSError(w, http.StatusNotFound, "not found")
		}

	default:
		writeNMOSError(w, http.StatusNotFound, "not found")
	}
}

// --- Store IS-05 Helpers ---

func (s *Store) hasSender(senderID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.bySenderID[senderID]
	return ok
}

func (s *Store) getSenderConstraints(senderID string) ([]map[string]ConstraintRule, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.bySenderID[senderID]
	if !ok {
		return nil, false
	}
	wsURI := fmt.Sprintf("ws://%s:%d/x-nmos/events/v1.0/ws", s.hostIP, s.httpPort)
	restURL := fmt.Sprintf("http://%s:%d/x-nmos/events/v1.0/sources/%s/", s.hostIP, s.httpPort, entry.Source.ID)
	c := []map[string]ConstraintRule{
		{
			"connection_uri":           {Enum: []any{wsURI}},
			"connection_authorization": {Enum: []any{false}},
			"ext_is_07_source_id":      {Enum: []any{entry.Source.ID}},
			"ext_is_07_rest_api_url":   {Enum: []any{restURL}},
		},
	}
	return c, true
}

func (s *Store) getSenderStaged(senderID string) (SenderConnectionState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.bySenderID[senderID]
	if !ok {
		return SenderConnectionState{}, false
	}
	return cloneConnState(entry.Staged), true
}

func (s *Store) getSenderActive(senderID string) (SenderConnectionState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.bySenderID[senderID]
	if !ok {
		return SenderConnectionState{}, false
	}
	return cloneConnState(entry.Active), true
}

func (s *Store) patchSenderStaged(senderID string, rawJSON []byte) (SenderConnectionState, int, string) {
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(rawJSON, &rawMap); err != nil {
		return SenderConnectionState{}, http.StatusBadRequest, "invalid JSON: " + err.Error()
	}

	s.mu.Lock()
	entry, ok := s.bySenderID[senderID]
	if !ok {
		s.mu.Unlock()
		return SenderConnectionState{}, http.StatusNotFound, "sender not found"
	}

	nextStaged := cloneConnState(entry.Staged)

	if rawRcv, present := rawMap["receiver_id"]; present {
		if bytes.Equal(bytes.TrimSpace(rawRcv), []byte("null")) {
			nextStaged.ReceiverID = nil
		} else {
			var rid string
			if err := json.Unmarshal(rawRcv, &rid); err != nil {
				s.mu.Unlock()
				return SenderConnectionState{}, http.StatusBadRequest, "invalid receiver_id"
			}
			nextStaged.ReceiverID = &rid
		}
	}

	if rawME, present := rawMap["master_enable"]; present {
		var me bool
		if err := json.Unmarshal(rawME, &me); err != nil {
			s.mu.Unlock()
			return SenderConnectionState{}, http.StatusBadRequest, "invalid master_enable"
		}
		nextStaged.MasterEnable = me
	}

	if rawTP, present := rawMap["transport_params"]; present {
		var tps []WebSocketTransportParams
		if err := json.Unmarshal(rawTP, &tps); err != nil || len(tps) == 0 {
			s.mu.Unlock()
			return SenderConnectionState{}, http.StatusBadRequest, "invalid transport_params"
		}
		wsURI := fmt.Sprintf("ws://%s:%d/x-nmos/events/v1.0/ws", s.hostIP, s.httpPort)
		restURL := fmt.Sprintf("http://%s:%d/x-nmos/events/v1.0/sources/%s/", s.hostIP, s.httpPort, entry.Source.ID)

		resolved := tps[0]
		if resolved.ConnectionURI == nil || resolved.ConnectionURI == "auto" || resolved.ConnectionURI == "" {
			resolved.ConnectionURI = wsURI
		}
		if resolved.ConnectionAuthorization == nil || resolved.ConnectionAuthorization == "auto" {
			resolved.ConnectionAuthorization = false
		}
		if resolved.ExtIS07SourceID == "" {
			resolved.ExtIS07SourceID = entry.Source.ID
		}
		if resolved.ExtIS07RestAPIURL == "" {
			resolved.ExtIS07RestAPIURL = restURL
		}
		nextStaged.TransportParams = []WebSocketTransportParams{resolved}
	}

	var actMode string
	var reqTimeStr string
	if rawAct, present := rawMap["activation"]; present && !bytes.Equal(bytes.TrimSpace(rawAct), []byte("null")) {
		var act ActivationParams
		if err := json.Unmarshal(rawAct, &act); err != nil {
			s.mu.Unlock()
			return SenderConnectionState{}, http.StatusBadRequest, "invalid activation object"
		}
		if act.Mode != nil {
			actMode = *act.Mode
		}
		if act.RequestedTime != nil {
			reqTimeStr = *act.RequestedTime
		}
		nextStaged.Activation = act
	} else {
		nextStaged.Activation = ActivationParams{Mode: nil, RequestedTime: nil}
	}

	switch actMode {
	case "":
		entry.Staged = nextStaged
		resp := cloneConnState(entry.Staged)
		s.mu.Unlock()
		return resp, http.StatusOK, ""

	case "activate_immediate":
		nowTAI := TAITimestampNow()
		nextStaged.Activation = ActivationParams{Mode: nil, RequestedTime: nil}
		entry.Staged = cloneConnState(nextStaged)

		nextActive := cloneConnState(nextStaged)
		modeCp := "activate_immediate"
		nextActive.Activation = ActivationParams{
			Mode:           &modeCp,
			RequestedTime:  nil,
			ActivationTime: &nowTAI,
		}
		entry.Active = nextActive
		entry.Sender.Subscription.ReceiverID = nextActive.ReceiverID
		entry.Sender.Subscription.Active = nextActive.MasterEnable
		entry.Sender.Version = nowTAI
		s.verSnd++

		resp := cloneConnState(entry.Staged)
		resp.Activation.Mode = &modeCp
		resp.Activation.ActivationTime = &nowTAI
		cb := s.onResourceChange
		s.mu.Unlock()
		if cb != nil {
			cb()
		}
		return resp, http.StatusOK, ""

	case "activate_scheduled_relative", "activate_scheduled_absolute":
		delay := parseScheduledDelay(actMode, reqTimeStr)
		scheduledAt := time.Now().Add(delay)
		schedTAI := FormatTAITimestamp(scheduledAt)
		modeCp := actMode
		reqCp := reqTimeStr
		nextStaged.Activation = ActivationParams{
			Mode:           &modeCp,
			RequestedTime:  &reqCp,
			ActivationTime: &schedTAI,
		}
		entry.Staged = cloneConnState(nextStaged)
		resp := cloneConnState(entry.Staged)
		s.mu.Unlock()

		time.AfterFunc(delay, func() {
			s.mu.Lock()
			e, exists := s.bySenderID[senderID]
			if !exists {
				s.mu.Unlock()
				return
			}
			actNow := TAITimestampNow()
			e.Active = cloneConnState(e.Staged)
			e.Active.Activation.ActivationTime = &actNow
			e.Staged.Activation = ActivationParams{Mode: nil, RequestedTime: nil}
			e.Sender.Subscription.ReceiverID = e.Active.ReceiverID
			e.Sender.Subscription.Active = e.Active.MasterEnable
			e.Sender.Version = actNow
			s.verSnd++
			cb := s.onResourceChange
			s.mu.Unlock()
			if cb != nil {
				cb()
			}
		})
		return resp, http.StatusAccepted, ""

	default:
		s.mu.Unlock()
		return SenderConnectionState{}, http.StatusBadRequest, "unsupported activation mode: " + actMode
	}
}

func parseScheduledDelay(mode, ts string) time.Duration {
	parts := strings.Split(ts, ":")
	if len(parts) != 2 {
		return 0
	}
	sec, err1 := strconv.ParseInt(parts[0], 10, 64)
	nsec, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0
	}
	if mode == "activate_scheduled_relative" {
		d := time.Duration(sec)*time.Second + time.Duration(nsec)*time.Nanosecond
		if d < 0 {
			return 0
		}
		return d
	}
	// Absolute TAI timestamp.
	utcSec := sec - CurrentTAIOffsetSeconds
	target := time.Unix(utcSec, nsec)
	d := time.Until(target)
	if d < 0 {
		return 0
	}
	return d
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeNMOSError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"code":  status,
		"error": msg,
		"debug": nil,
	})
}
