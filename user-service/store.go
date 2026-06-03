package main

import (
	"database/sql"
	"time"
)

type User struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	CreatedAt string `json:"created_at"`
}

type store struct {
	db *sql.DB
}

func (s *store) create(name, email string) (User, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := s.db.Exec(
		`INSERT INTO users (name, email, created_at) VALUES (?, ?, ?)`,
		name, email, now,
	)
	if err != nil {
		return User{}, err
	}
	id, _ := res.LastInsertId()
	return User{ID: id, Name: name, Email: email, CreatedAt: now}, nil
}

func (s *store) get(id int64) (User, error) {
	var u User
	err := s.db.QueryRow(
		`SELECT id, name, email, created_at FROM users WHERE id = ?`, id,
	).Scan(&u.ID, &u.Name, &u.Email, &u.CreatedAt)
	return u, err
}
