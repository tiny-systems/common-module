package kv_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tiny-systems/common-module/components/kv"
	"github.com/tiny-systems/common-module/internal/testharness"
)

func newKV() *testharness.Harness {
	return testharness.New((&kv.Component{}).Instance())
}

func storeDoc(t *testing.T, h *testharness.Harness, doc kv.Document) {
	t.Helper()
	result := h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpStore,
		Document:  doc,
	})
	if err, ok := result.(error); ok {
		t.Fatalf("store failed: %v", err)
	}
}

func queryAll(t *testing.T, h *testharness.Harness) kv.QueryResult {
	t.Helper()
	h.Reset()
	h.Handle(context.Background(), kv.QueryPort, kv.QueryRequest{})
	outs := h.PortOutputs(kv.QueryResultPort)
	if len(outs) != 1 {
		t.Fatalf("expected 1 query result, got %d", len(outs))
	}
	return outs[0].(kv.QueryResult)
}

func TestStoreAndQueryAll(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "url": "https://example.com", "status": "UP"})

	qr := queryAll(t, h)
	if qr.Count != 1 {
		t.Fatalf("count: got %d, want 1", qr.Count)
	}
	if qr.Results[0].Key != "ep1" {
		t.Errorf("key: got %q, want ep1", qr.Results[0].Key)
	}
	if qr.Results[0].Document["status"] != "UP" {
		t.Errorf("status: got %v, want UP", qr.Results[0].Document["status"])
	}
}

func TestQueryByJSONPath(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})
	storeDoc(t, h, kv.Document{"id": "ep2", "status": "DOWN"})

	h.Reset()
	h.Handle(context.Background(), kv.QueryPort, kv.QueryRequest{
		Query: "$.status == 'DOWN'",
	})
	outs := h.PortOutputs(kv.QueryResultPort)
	if len(outs) != 1 {
		t.Fatalf("expected 1 query result output, got %d", len(outs))
	}
	qr := outs[0].(kv.QueryResult)
	if qr.Count != 1 {
		t.Fatalf("count: got %d, want 1", qr.Count)
	}
	if qr.Results[0].Key != "ep2" {
		t.Errorf("key: got %q, want ep2", qr.Results[0].Key)
	}
}

func TestDelete(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})

	h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpDelete,
		Document:  kv.Document{"id": "ep1"},
	})

	qr := queryAll(t, h)
	if qr.Count != 0 {
		t.Errorf("count after delete: got %d, want 0", qr.Count)
	}
}

func TestMetadataPersistence(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})

	raw, ok := h.Metadata["kv-ep1"]
	if !ok {
		t.Fatal("kv-ep1 not in metadata")
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if doc["status"] != "UP" {
		t.Errorf("status in metadata: got %v, want UP", doc["status"])
	}
}

func TestDeleteRemovesMetadata(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})

	h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpDelete,
		Document:  kv.Document{"id": "ep1"},
	})

	if _, ok := h.Metadata["kv-ep1"]; ok {
		t.Error("kv-ep1 should be removed from metadata after delete")
	}
}

func TestPodRestart(t *testing.T) {
	ctx := context.Background()
	pod1 := newKV()
	storeDoc(t, pod1, kv.Document{"id": "ep1", "status": "DOWN"})

	pod2 := pod1.NewPod()
	pod2.Reconcile(ctx)

	qr := queryAll(t, pod2)
	if qr.Count != 1 {
		t.Fatalf("pod2 count: got %d, want 1", qr.Count)
	}
	if qr.Results[0].Document["status"] != "DOWN" {
		t.Errorf("pod2 status: got %v, want DOWN", qr.Results[0].Document["status"])
	}
}

func TestStaleReconcileDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	h := newKV()

	// Store via port sets settingsFromPort guard
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})

	// Inject stale metadata and reconcile
	h.Metadata["kv-ep1"] = `{"id":"ep1","status":"STALE"}`
	h.Reconcile(ctx)

	qr := queryAll(t, h)
	if qr.Count != 1 {
		t.Fatalf("count: got %d, want 1", qr.Count)
	}
	if qr.Results[0].Document["status"] != "UP" {
		t.Errorf("stale reconcile overwrote: got %v, want UP", qr.Results[0].Document["status"])
	}
}

func TestMaxRecords(t *testing.T) {
	h := newKV()

	// Set max to 2 via settings
	h.Handle(context.Background(), "_settings", kv.Settings{
		Document:   kv.Document{"id": ""},
		PrimaryKey: "id",
		MaxRecords: 2,
	})

	storeDoc(t, h, kv.Document{"id": "a"})
	storeDoc(t, h, kv.Document{"id": "b"})

	// Third should fail
	result := h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpStore,
		Document:  kv.Document{"id": "c"},
	})
	if err, ok := result.(error); !ok {
		t.Fatal("expected error when store is full")
	} else if !strings.Contains(err.Error(), "store full") {
		t.Errorf("unexpected error: %v", err)
	}

	// Update existing should still work
	storeDoc(t, h, kv.Document{"id": "a", "updated": "yes"})
}

func TestDocumentTooLarge(t *testing.T) {
	h := newKV()
	bigValue := strings.Repeat("x", 33*1024) // >32KB
	result := h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpStore,
		Document:  kv.Document{"id": "big", "data": bigValue},
	})
	if err, ok := result.(error); !ok {
		t.Fatal("expected error for oversized document")
	} else if !strings.Contains(err.Error(), "too large") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEmptyPrimaryKey(t *testing.T) {
	h := newKV()
	result := h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpStore,
		Document:  kv.Document{"id": ""},
	})
	if _, ok := result.(error); !ok {
		t.Fatal("expected error for empty primary key")
	}
}

func TestMissingPrimaryKey(t *testing.T) {
	h := newKV()
	result := h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Operation: kv.OpStore,
		Document:  kv.Document{"name": "no-pk"},
	})
	if _, ok := result.(error); !ok {
		t.Fatal("expected error for missing primary key")
	}
}

func TestUpdateExistingKey(t *testing.T) {
	h := newKV()
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})
	storeDoc(t, h, kv.Document{"id": "ep1", "status": "DOWN"})

	qr := queryAll(t, h)
	if qr.Count != 1 {
		t.Fatalf("count: got %d, want 1", qr.Count)
	}
	if qr.Results[0].Document["status"] != "DOWN" {
		t.Errorf("status: got %v, want DOWN", qr.Results[0].Document["status"])
	}
}

func TestQueryEmptyStore(t *testing.T) {
	h := newKV()
	qr := queryAll(t, h)
	if qr.Count != 0 {
		t.Errorf("count: got %d, want 0", qr.Count)
	}
	if len(qr.Results) != 0 {
		t.Errorf("results: got %d items, want 0", len(qr.Results))
	}
}

func TestStoreAckPort(t *testing.T) {
	h := newKV()
	h.Handle(context.Background(), "_settings", kv.Settings{
		Document:       kv.Document{"id": ""},
		PrimaryKey:     "id",
		MaxRecords:     100,
		EnableStoreAck: true,
	})

	storeDoc(t, h, kv.Document{"id": "ep1", "status": "UP"})

	acks := h.PortOutputs(kv.StoreAckPort)
	if len(acks) != 1 {
		t.Fatalf("expected 1 ack, got %d", len(acks))
	}
	ack := acks[0].(kv.StoreAck)
	if ack.Request.Document["id"] != "ep1" {
		t.Errorf("ack document id: got %v, want ep1", ack.Request.Document["id"])
	}
}

func TestStoreAckPortHiddenByDefault(t *testing.T) {
	h := newKV()
	for _, p := range h.Ports() {
		if p.Name == kv.StoreAckPort {
			t.Fatal("store_ack port should not be visible by default")
		}
	}
}

func TestContextPassthrough(t *testing.T) {
	h := newKV()
	h.Handle(context.Background(), kv.StorePort, kv.StoreRequest{
		Context:   "my-ctx",
		Operation: kv.OpStore,
		Document:  kv.Document{"id": "ep1"},
	})

	h.Handle(context.Background(), kv.QueryPort, kv.QueryRequest{
		Context: "query-ctx",
	})

	outs := h.PortOutputs(kv.QueryResultPort)
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	qr := outs[0].(kv.QueryResult)
	if qr.Context != "query-ctx" {
		t.Errorf("context: got %v, want query-ctx", qr.Context)
	}
}

func TestPodRestartMultipleKeys(t *testing.T) {
	ctx := context.Background()
	pod1 := newKV()

	storeDoc(t, pod1, kv.Document{"id": "ep1", "status": "UP"})
	storeDoc(t, pod1, kv.Document{"id": "ep2", "status": "DOWN"})
	storeDoc(t, pod1, kv.Document{"id": "ep3", "status": "UP"})

	pod2 := pod1.NewPod()
	pod2.Reconcile(ctx)

	qr := queryAll(t, pod2)
	if qr.Count != 3 {
		t.Fatalf("pod2 count: got %d, want 3", qr.Count)
	}
}
