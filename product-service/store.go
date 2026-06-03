package main

import (
	"database/sql"
	"time"
)

type Product struct {
	ID        int64   `json:"id"`
	Name      string  `json:"name"`
	Price     float64 `json:"price"`
	CreatedAt string  `json:"created_at"`
}

type store struct {
	db *sql.DB
}

func (s *store) create(name string, price float64) (Product, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := s.db.Exec(
		`INSERT INTO products (name, price, created_at) VALUES (?, ?, ?)`,
		name, price, now,
	)
	if err != nil {
		return Product{}, err
	}
	id, _ := res.LastInsertId()
	return Product{ID: id, Name: name, Price: price, CreatedAt: now}, nil
}

func (s *store) get(id int64) (Product, error) {
	var p Product
	err := s.db.QueryRow(
		`SELECT id, name, price, created_at FROM products WHERE id = ?`, id,
	).Scan(&p.ID, &p.Name, &p.Price, &p.CreatedAt)
	return p, err
}
