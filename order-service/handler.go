package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"
)

type createOrderReq struct {
	UserID int64 `json:"user_id"`
	Items  []struct {
		ProductID int64 `json:"product_id"`
		Quantity  int   `json:"quantity"`
	} `json:"items"`
}

func (s *orderStore) handleCreate(w http.ResponseWriter, r *http.Request) {
	// ── Idempotency check ────────────────────────────────────────────────────
	// If the gateway retried this request (same Idempotency-Key), we return
	// the cached result instead of creating a duplicate order.
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey != "" {
		if e, ok := globalIdem.get(idemKey); ok {
			slog.Info("idempotent replay", "key", idemKey)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Idempotent-Replayed", "true")
			w.WriteHeader(e.statusCode)
			w.Write(e.body)
			return
		}
	}

	var req createOrderReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	if len(req.Items) == 0 {
		writeErr(w, http.StatusBadRequest, "items required")
		return
	}

	// ── Fetch current prices from product-service ────────────────────────────
	// We call product-service directly (service-to-service), not through the
	// gateway. The price is snapshotted here so the order total is immutable
	// even if the product price changes later.
	inputs := make([]itemInput, len(req.Items))
	for i, item := range req.Items {
		price, err := fetchProductPrice(r.Context(), item.ProductID)
		if err != nil {
			slog.Warn("product fetch failed", "product_id", item.ProductID, "err", err)
			writeErr(w, http.StatusBadGateway, fmt.Sprintf("product %d: %s", item.ProductID, err))
			return
		}
		inputs[i] = itemInput{ProductID: item.ProductID, Quantity: item.Quantity, Price: price}
	}

	order, err := s.create(req.UserID, inputs)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	body, _ := json.Marshal(order)
	statusCode := http.StatusCreated

	// ── Store result for future idempotent replays ───────────────────────────
	if idemKey != "" {
		globalIdem.set(idemKey, statusCode, body)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(body)
}

func (s *orderStore) handleGet(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	o, err := s.get(id)
	if errors.Is(err, sql.ErrNoRows) {
		writeErr(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(o)
}

// fetchProductPrice calls product-service and returns the current price for
// the given product ID. It honours the parent context's deadline so the whole
// request chain shares one timeout budget.
func fetchProductPrice(ctx context.Context, productID int64) (float64, error) {
	base := os.Getenv("PRODUCT_SERVICE_URL")
	if base == "" {
		base = "http://localhost:8082"
	}
	url := fmt.Sprintf("%s/products/%d", base, productID)

	// Give the sub-call its own deadline without exceeding the parent's.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return 0, fmt.Errorf("not found")
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}

	var p struct {
		Price float64 `json:"price"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return 0, err
	}
	return p.Price, nil
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
