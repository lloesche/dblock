package dblock

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

const (
	baseLockID    = 6877
	checkInterval = 5 * time.Second
)

func UpgradeIfNeeded(db *sql.DB, targetVersion int, upgradeFunc func(*sql.Tx) error, timeout time.Duration) error {
	currentVersion, err := getSchemaVersion(db)
	if err != nil {
		return err
	}

	if currentVersion >= targetVersion {
		log.Printf("No upgrade needed. Current version: %d\n", currentVersion)
		return nil
	}

	lockID := baseLockID + targetVersion

	if err := acquireAdvisoryLock(db, lockID); err != nil {
		log.Println("Another instance is handling the upgrade.")
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			time.Sleep(checkInterval)

			latestVersion, err := getSchemaVersion(db)
			if err != nil {
				return err
			}

			if latestVersion >= targetVersion {
				log.Println("Schema was upgraded by another instance.")
				return nil
			}
		}

		return logErrorf("Timeout: schema upgrade was not completed in %v", timeout)
	}
	defer func() {
		_ = releaseAdvisoryLock(db, lockID)
	}()

	// Double-check version after acquiring lock
	latestVersion, err := getSchemaVersion(db)
	if err != nil {
		return err
	}

	if latestVersion >= targetVersion {
		log.Println("Another instance already upgraded the schema.")
		return nil
	}

	log.Printf("Upgrading schema to version %d...\n", targetVersion)
	if err := upgradeSchema(db, targetVersion, upgradeFunc); err != nil {
		return err
	}

	log.Println("Upgrade complete.")
	return nil
}

func getSchemaVersion(db *sql.DB) (int, error) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_version (
			version INTEGER NOT NULL DEFAULT 0
		);
		INSERT INTO schema_version (version) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM schema_version);
	`)
	if err != nil {
		return 0, logErrorf("Failed to initialize schema_version table: %v", err)
	}

	var version int
	err = db.QueryRow("SELECT version FROM schema_version").Scan(&version)
	if err != nil {
		return 0, logErrorf("Failed to get schema version: %v", err)
	}

	return version, nil
}

func upgradeSchema(db *sql.DB, newVersion int, upgradeFunc func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return logErrorf("Failed to start transaction: %w", err)
	}

	if err := upgradeFunc(tx); err != nil {
		_ = tx.Rollback()
		return logErrorf("Failed to modify schema: %w", err)
	}

	if _, err := tx.Exec("UPDATE schema_version SET version = $1", newVersion); err != nil {
		_ = tx.Rollback()
		return logErrorf("Failed to update schema version: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return logErrorf("Failed to commit transaction: %w", err)
	}

	return nil
}

func acquireAdvisoryLock(db *sql.DB, lockID int) error {
	var acquired bool
	err := db.QueryRow("SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
	if err != nil {
		return logErrorf("Failed to check advisory lock: %v", err)
	}
	if !acquired {
		return logErrorf("Advisory lock is already held by another process!")
	}
	return nil
}

func releaseAdvisoryLock(db *sql.DB, lockID int) error {
	_, err := db.Exec("SELECT pg_advisory_unlock($1)", lockID)
	if err != nil {
		return logErrorf("Failed to release advisory lock: %w", err)
	}
	return nil
}

func logErrorf(format string, v ...interface{}) error {
	err := fmt.Errorf(format, v...)
	log.Println(err)
	return err
}
