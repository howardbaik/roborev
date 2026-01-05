package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

const currentSchemaVersion = 2

const schema = `
CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS repos (
  id INTEGER PRIMARY KEY,
  root_path TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS commits (
  id INTEGER PRIMARY KEY,
  repo_id INTEGER NOT NULL REFERENCES repos(id),
  sha TEXT UNIQUE NOT NULL,
  author TEXT NOT NULL,
  subject TEXT NOT NULL,
  timestamp TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS review_jobs (
  id INTEGER PRIMARY KEY,
  repo_id INTEGER NOT NULL REFERENCES repos(id),
  commit_id INTEGER REFERENCES commits(id),
  git_ref TEXT NOT NULL,
  agent TEXT NOT NULL DEFAULT 'codex',
  status TEXT NOT NULL CHECK(status IN ('queued','running','done','failed')) DEFAULT 'queued',
  enqueued_at TEXT NOT NULL DEFAULT (datetime('now')),
  started_at TEXT,
  finished_at TEXT,
  worker_id TEXT,
  error TEXT
);

CREATE TABLE IF NOT EXISTS reviews (
  id INTEGER PRIMARY KEY,
  job_id INTEGER UNIQUE NOT NULL REFERENCES review_jobs(id),
  agent TEXT NOT NULL,
  prompt TEXT NOT NULL,
  output TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS responses (
  id INTEGER PRIMARY KEY,
  commit_id INTEGER NOT NULL REFERENCES commits(id),
  responder TEXT NOT NULL,
  response TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_review_jobs_status ON review_jobs(status);
CREATE INDEX IF NOT EXISTS idx_review_jobs_repo ON review_jobs(repo_id);
CREATE INDEX IF NOT EXISTS idx_review_jobs_git_ref ON review_jobs(git_ref);
CREATE INDEX IF NOT EXISTS idx_commits_sha ON commits(sha);
`

type DB struct {
	*sql.DB
}

// DefaultDBPath returns the default database path
func DefaultDBPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".roborev", "reviews.db")
}

// Open opens or creates the database at the given path
func Open(dbPath string) (*DB, error) {
	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	// Open with WAL mode and busy timeout
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	wrapped := &DB{db}

	// Run migrations
	if err := wrapped.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate database: %w", err)
	}

	return wrapped, nil
}

// migrate runs database migrations
func (db *DB) migrate() error {
	// Check if this is a v1 database (has review_jobs table but no schema_version)
	if db.isV1Database() {
		if err := db.migrateV1ToV2(); err != nil {
			return fmt.Errorf("migrate v1 to v2: %w", err)
		}
		return db.setSchemaVersion(currentSchemaVersion)
	}

	// Get current version (0 if fresh database)
	version := db.getSchemaVersion()

	// Fresh database - just run the schema
	if version == 0 {
		if _, err := db.Exec(schema); err != nil {
			return fmt.Errorf("run schema: %w", err)
		}
		return db.setSchemaVersion(currentSchemaVersion)
	}

	// Already at current version
	if version >= currentSchemaVersion {
		return nil
	}

	// Future migrations would go here
	return db.setSchemaVersion(currentSchemaVersion)
}

// isV1Database checks if this is a v1 database (has commit_sha column, no schema_version)
func (db *DB) isV1Database() bool {
	// Check if review_jobs table exists with commit_sha column
	rows, err := db.Query("PRAGMA table_info(review_jobs)")
	if err != nil {
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false
		}
		if name == "commit_sha" {
			return true
		}
	}
	return false
}

// getSchemaVersion returns the current schema version (0 if not set)
func (db *DB) getSchemaVersion() int {
	var version int
	err := db.QueryRow("SELECT version FROM schema_version LIMIT 1").Scan(&version)
	if err != nil {
		return 0
	}
	return version
}

// setSchemaVersion sets the schema version
func (db *DB) setSchemaVersion(version int) error {
	_, err := db.Exec("DELETE FROM schema_version")
	if err != nil {
		return err
	}
	_, err = db.Exec("INSERT INTO schema_version (version) VALUES (?)", version)
	return err
}

// migrateV1ToV2 migrates from schema v1 (commit_sha) to v2 (git_ref)
func (db *DB) migrateV1ToV2() error {
	// Check if we have the old commit_sha column
	var hasCommitSHA bool
	rows, err := db.Query("PRAGMA table_info(review_jobs)")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == "commit_sha" {
			hasCommitSHA = true
			break
		}
	}

	if !hasCommitSHA {
		// Already migrated or fresh schema, ensure schema_version table exists
		_, err := db.Exec("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
		return err
	}

	// SQLite doesn't support RENAME COLUMN in older versions, so we need to:
	// 1. Create new table with correct schema
	// 2. Copy data
	// 3. Drop old table
	// 4. Rename new table

	_, err = db.Exec(`
		-- Create schema_version table
		CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);

		-- Create new review_jobs table with git_ref instead of commit_sha
		CREATE TABLE review_jobs_new (
			id INTEGER PRIMARY KEY,
			repo_id INTEGER NOT NULL REFERENCES repos(id),
			commit_id INTEGER REFERENCES commits(id),
			git_ref TEXT NOT NULL,
			agent TEXT NOT NULL DEFAULT 'codex',
			status TEXT NOT NULL CHECK(status IN ('queued','running','done','failed')) DEFAULT 'queued',
			enqueued_at TEXT NOT NULL DEFAULT (datetime('now')),
			started_at TEXT,
			finished_at TEXT,
			worker_id TEXT,
			error TEXT
		);

		-- Copy data, renaming commit_sha to git_ref
		INSERT INTO review_jobs_new (id, repo_id, commit_id, git_ref, agent, status, enqueued_at, started_at, finished_at, worker_id, error)
		SELECT id, repo_id, commit_id, commit_sha, agent, status, enqueued_at, started_at, finished_at, worker_id, error
		FROM review_jobs;

		-- Drop old table
		DROP TABLE review_jobs;

		-- Rename new table
		ALTER TABLE review_jobs_new RENAME TO review_jobs;

		-- Recreate indexes
		CREATE INDEX IF NOT EXISTS idx_review_jobs_status ON review_jobs(status);
		CREATE INDEX IF NOT EXISTS idx_review_jobs_repo ON review_jobs(repo_id);
		CREATE INDEX IF NOT EXISTS idx_review_jobs_git_ref ON review_jobs(git_ref);
	`)

	return err
}

// ResetStaleJobs marks all running jobs as queued (for daemon restart)
func (db *DB) ResetStaleJobs() error {
	_, err := db.Exec(`
		UPDATE review_jobs
		SET status = 'queued', worker_id = NULL, started_at = NULL
		WHERE status = 'running'
	`)
	return err
}
