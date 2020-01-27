package datastore

import (
	"database/sql"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

const (
	createTelemetry = `
	CREATE TABLE IF NOT EXISTS telemetry
	(id INT PRIMARY KEY,
	 temperature REAL,
	 humidity REAL,
	 timestamp INT,
	 sent_to_cloud BOOLEAN default 0)`
)

type TelemetryValues struct {
	Temperature float32 `json:"temperature"`
	Humidity    float32 `json:"humidity"`
	Timestamp   int64   `json:"timestamp"`
}

type Datastore struct {
	db      *sql.DB
	dbMutex *sync.Mutex
}

func New() (Datastore, error) {
	db, err := createDb("./")
	if err != nil {
		return Datastore{}, err
	}
	mutex := sync.Mutex{}
	return Datastore{
		dbMutex: &mutex,
		db:      db,
	}, nil
}

func (ds *Datastore) SaveValues(telemetry TelemetryValues) error {
	log.Println("[ds.SaveValues] saving telemetry")
	stmt := `INSERT or IGNORE into telemetry (temperature,
		humidity,
		timestamp) VALUES (?, ?, ?)`

	dbTX, err := ds.db.Begin()
	s, err := dbTX.Prepare(stmt)
	if err != nil {
		return err
	}

	defer s.Close()

	_, err = s.Exec(telemetry.Temperature, telemetry.Humidity, telemetry.Timestamp)
	if err != nil {
		dbTX.Rollback()
		return err
	}

	err = dbTX.Commit()
	if err != nil {
		dbTX.Rollback()
		return err
	}

	return nil
}

func createDb(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	err = createTables(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func createTables(db *sql.DB) error {
	dbTX, err := db.Begin()
	if err != nil {
		return err
	}

	s, err := dbTX.Prepare(createTelemetry)
	if err != nil {
		return err
	}

	defer s.Close()

	_, err = s.Exec()
	if err != nil {
		dbTX.Rollback()
		return err
	}

	err = dbTX.Commit()
	if err != nil {
		dbTX.Rollback()
		return err
	}

	return nil
}
