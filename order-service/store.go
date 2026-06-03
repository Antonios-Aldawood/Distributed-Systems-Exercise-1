package main

import (
	"database/sql"
	"time"
)

type Order struct {
	ID        int64       `json:"id"`
	UserID    int64       `json:"user_id"`
	Status    string      `json:"status"`
	Items     []OrderItem `json:"items"`
	CreatedAt string      `json:"created_at"`
}

type OrderItem struct {
	ID          int64   `json:"id"`
	OrderID     int64   `json:"order_id"`
	ProductID   int64   `json:"product_id"`
	Quantity    int     `json:"quantity"`
	PriceAtTime float64 `json:"price_at_time"`
}

type itemInput struct {
	ProductID int64
	Quantity  int
	Price     float64
}

type orderStore struct {
	db *sql.DB
}

func (s *orderStore) create(userID int64, items []itemInput) (Order, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	tx, err := s.db.Begin()
	if err != nil {
		return Order{}, err
	}
	defer tx.Rollback()

	res, err := tx.Exec(
		`INSERT INTO orders (user_id, status, created_at) VALUES (?, 'pending', ?)`,
		userID, now,
	)
	if err != nil {
		return Order{}, err
	}
	orderID, _ := res.LastInsertId()

	var orderItems []OrderItem
	for _, it := range items {
		r, err := tx.Exec(
			`INSERT INTO order_items (order_id, product_id, quantity, price_at_time) VALUES (?, ?, ?, ?)`,
			orderID, it.ProductID, it.Quantity, it.Price,
		)
		if err != nil {
			return Order{}, err
		}
		itemID, _ := r.LastInsertId()
		orderItems = append(orderItems, OrderItem{
			ID:          itemID,
			OrderID:     orderID,
			ProductID:   it.ProductID,
			Quantity:    it.Quantity,
			PriceAtTime: it.Price,
		})
	}

	if err := tx.Commit(); err != nil {
		return Order{}, err
	}

	return Order{
		ID:        orderID,
		UserID:    userID,
		Status:    "pending",
		Items:     orderItems,
		CreatedAt: now,
	}, nil
}

func (s *orderStore) get(id int64) (Order, error) {
	var o Order
	err := s.db.QueryRow(
		`SELECT id, user_id, status, created_at FROM orders WHERE id = ?`, id,
	).Scan(&o.ID, &o.UserID, &o.Status, &o.CreatedAt)
	if err != nil {
		return Order{}, err
	}

	rows, err := s.db.Query(
		`SELECT id, order_id, product_id, quantity, price_at_time FROM order_items WHERE order_id = ?`, id,
	)
	if err != nil {
		return Order{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var it OrderItem
		if err := rows.Scan(&it.ID, &it.OrderID, &it.ProductID, &it.Quantity, &it.PriceAtTime); err != nil {
			return Order{}, err
		}
		o.Items = append(o.Items, it)
	}
	return o, rows.Err()
}
