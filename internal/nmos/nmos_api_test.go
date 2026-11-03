package nmos_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"tsl2nmos/internal/config"
	"tsl2nmos/internal/nmos"
	"tsl2nmos/internal/tally"
)

func TestDeterministicUUIDStabilityAndIS04IS05IS07APIs(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Node.IDSeed = "studio-fixed-seed"
	cfg.Node.HostIP = "192.168.10.20"
	cfg.Node.HTTPPort = 8080

	store1 := nmos.NewStore(cfg)
	store2 := nmos.NewStore(cfg)

	n1, d1, s1, _, snd1 := store1.SnapshotResources()
	n2, d2, s2, _, snd2 := store2.SnapshotResources()

	if n1.ID != n2.ID || d1.ID != d2.ID || s1[0].ID != s2[0].ID || snd1[0].ID != snd2[0].ID {
		t.Fatalf("UUIDv5 resources did not remain deterministic across restarts")
	}

	wsHub := nmos.NewWSHub(store1, nil)
	mux := http.NewServeMux()
	nmos.RegisterRoutes(mux, store1, wsHub)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// 1. Check IS-04 /x-nmos/node/v1.3/sources
	res, err := http.Get(srv.URL + "/x-nmos/node/v1.3/sources")
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("GET /sources failed: %v status=%d", err, res.StatusCode)
	}
	if res.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("missing NMOS CORS header")
	}
	var gotSources []nmos.SourceResource
	_ = json.NewDecoder(res.Body).Decode(&gotSources)
	_ = res.Body.Close()
	if len(gotSources) != 16 {
		t.Fatalf("expected 16 boolean sources for 8 default channels, got %d", len(gotSources))
	}
	if gotSources[0].Format != "urn:x-nmos:format:data" || gotSources[0].EventType != "boolean" {
		t.Fatalf("unexpected IS-04 source attributes: %+v", gotSources[0])
	}

	// 2. Check IS-07 /x-nmos/events/v1.0/sources/{id}/type
	srcID := nmos.ChannelSourceID(cfg.Node.IDSeed, 1, tally.ColorRed)
	res, err = http.Get(srv.URL + "/x-nmos/events/v1.0/sources/" + srcID + "/type/")
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("GET IS-07 /type failed: %v", err)
	}
	var typeDef map[string]any
	_ = json.NewDecoder(res.Body).Decode(&typeDef)
	_ = res.Body.Close()
	if typeDef["type"] != "boolean" {
		t.Fatalf("expected type=boolean, got %+v", typeDef)
	}

	// 3. Check IS-05 Bulk Senders + Scheduled Relative Activation
	sndID := nmos.ChannelSenderID(cfg.Node.IDSeed, 1, tally.ColorRed)
	bulkBody := `[{"id":"` + sndID + `","params":{"master_enable":false,"activation":{"mode":"activate_scheduled_relative","requested_time":"0:30000000"}}}]`
	res, err = http.Post(srv.URL+"/x-nmos/connection/v1.1/bulk/senders/", "application/json", bytes.NewBufferString(bulkBody))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("POST bulk/senders failed: %v status=%d", err, res.StatusCode)
	}
	var bulkResp []nmos.BulkResponseItem
	_ = json.NewDecoder(res.Body).Decode(&bulkResp)
	_ = res.Body.Close()
	if len(bulkResp) != 1 || bulkResp[0].Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for scheduled relative activation, got %+v", bulkResp)
	}

	time.Sleep(60 * time.Millisecond)

	res, err = http.Get(srv.URL + "/x-nmos/connection/v1.1/single/senders/" + sndID + "/active/")
	if err != nil {
		t.Fatalf("GET /active failed: %v", err)
	}
	var active nmos.SenderConnectionState
	_ = json.NewDecoder(res.Body).Decode(&active)
	_ = res.Body.Close()
	if active.MasterEnable {
		t.Fatalf("expected scheduled activation to set master_enable=false")
	}
}
