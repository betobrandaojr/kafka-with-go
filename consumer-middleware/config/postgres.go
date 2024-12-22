package config

import (
	"database/sql"

	_ "github.com/lib/pq"
)

func ConnectPostgres() (*sql.DB, error) {
	connStr := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	pgDB, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err = pgDB.Ping(); err != nil {
		return nil, err
	}
	return pgDB, nil
}
