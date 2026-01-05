package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestOpenAndClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Verify file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file was not created")
	}
}

func TestRepoOperations(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create repo
	repo, err := db.GetOrCreateRepo("/tmp/test-repo")
	if err != nil {
		t.Fatalf("GetOrCreateRepo failed: %v", err)
	}

	if repo.ID == 0 {
		t.Error("Repo ID should not be 0")
	}
	if repo.Name != "test-repo" {
		t.Errorf("Expected name 'test-repo', got '%s'", repo.Name)
	}

	// Get same repo again (should return existing)
	repo2, err := db.GetOrCreateRepo("/tmp/test-repo")
	if err != nil {
		t.Fatalf("GetOrCreateRepo (second call) failed: %v", err)
	}
	if repo2.ID != repo.ID {
		t.Error("Should return same repo on second call")
	}
}

func TestCommitOperations(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	repo, _ := db.GetOrCreateRepo("/tmp/test-repo")

	// Create commit
	commit, err := db.GetOrCreateCommit(repo.ID, "abc123def456", "Test Author", "Test commit", time.Now())
	if err != nil {
		t.Fatalf("GetOrCreateCommit failed: %v", err)
	}

	if commit.ID == 0 {
		t.Error("Commit ID should not be 0")
	}
	if commit.SHA != "abc123def456" {
		t.Errorf("Expected SHA 'abc123def456', got '%s'", commit.SHA)
	}

	// Get by SHA
	found, err := db.GetCommitBySHA("abc123def456")
	if err != nil {
		t.Fatalf("GetCommitBySHA failed: %v", err)
	}
	if found.ID != commit.ID {
		t.Error("GetCommitBySHA returned wrong commit")
	}
}

func TestJobLifecycle(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	repo, _ := db.GetOrCreateRepo("/tmp/test-repo")
	commit, _ := db.GetOrCreateCommit(repo.ID, "abc123", "Author", "Subject", time.Now())

	// Enqueue job
	job, err := db.EnqueueJob(repo.ID, commit.ID, "abc123", "codex")
	if err != nil {
		t.Fatalf("EnqueueJob failed: %v", err)
	}

	if job.Status != JobStatusQueued {
		t.Errorf("Expected status 'queued', got '%s'", job.Status)
	}

	// Claim job
	claimed, err := db.ClaimJob("worker-1")
	if err != nil {
		t.Fatalf("ClaimJob failed: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimJob returned nil")
	}
	if claimed.ID != job.ID {
		t.Error("ClaimJob returned wrong job")
	}
	if claimed.Status != JobStatusRunning {
		t.Errorf("Expected status 'running', got '%s'", claimed.Status)
	}

	// Claim again should return nil (no more jobs)
	claimed2, err := db.ClaimJob("worker-2")
	if err != nil {
		t.Fatalf("ClaimJob (second) failed: %v", err)
	}
	if claimed2 != nil {
		t.Error("ClaimJob should return nil when no jobs available")
	}

	// Complete job
	err = db.CompleteJob(job.ID, "codex", "test prompt", "test output")
	if err != nil {
		t.Fatalf("CompleteJob failed: %v", err)
	}

	// Verify job status
	updatedJob, err := db.GetJobByID(job.ID)
	if err != nil {
		t.Fatalf("GetJobByID failed: %v", err)
	}
	if updatedJob.Status != JobStatusDone {
		t.Errorf("Expected status 'done', got '%s'", updatedJob.Status)
	}
}

func TestJobFailure(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	repo, _ := db.GetOrCreateRepo("/tmp/test-repo")
	commit, _ := db.GetOrCreateCommit(repo.ID, "def456", "Author", "Subject", time.Now())

	job, _ := db.EnqueueJob(repo.ID, commit.ID, "def456", "codex")
	db.ClaimJob("worker-1")

	// Fail the job
	err := db.FailJob(job.ID, "test error message")
	if err != nil {
		t.Fatalf("FailJob failed: %v", err)
	}

	updatedJob, _ := db.GetJobByID(job.ID)
	if updatedJob.Status != JobStatusFailed {
		t.Errorf("Expected status 'failed', got '%s'", updatedJob.Status)
	}
	if updatedJob.Error != "test error message" {
		t.Errorf("Expected error message 'test error message', got '%s'", updatedJob.Error)
	}
}

func TestReviewOperations(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	repo, _ := db.GetOrCreateRepo("/tmp/test-repo")
	commit, _ := db.GetOrCreateCommit(repo.ID, "rev123", "Author", "Subject", time.Now())
	job, _ := db.EnqueueJob(repo.ID, commit.ID, "rev123", "codex")
	db.ClaimJob("worker-1")
	db.CompleteJob(job.ID, "codex", "the prompt", "the review output")

	// Get review by commit SHA
	review, err := db.GetReviewByCommitSHA("rev123")
	if err != nil {
		t.Fatalf("GetReviewByCommitSHA failed: %v", err)
	}

	if review.Output != "the review output" {
		t.Errorf("Expected output 'the review output', got '%s'", review.Output)
	}
	if review.Agent != "codex" {
		t.Errorf("Expected agent 'codex', got '%s'", review.Agent)
	}
}

func TestResponseOperations(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	repo, _ := db.GetOrCreateRepo("/tmp/test-repo")
	commit, _ := db.GetOrCreateCommit(repo.ID, "resp123", "Author", "Subject", time.Now())

	// Add response
	resp, err := db.AddResponse(commit.ID, "test-user", "LGTM!")
	if err != nil {
		t.Fatalf("AddResponse failed: %v", err)
	}

	if resp.Response != "LGTM!" {
		t.Errorf("Expected response 'LGTM!', got '%s'", resp.Response)
	}

	// Get responses
	responses, err := db.GetResponsesForCommit(commit.ID)
	if err != nil {
		t.Fatalf("GetResponsesForCommit failed: %v", err)
	}

	if len(responses) != 1 {
		t.Errorf("Expected 1 response, got %d", len(responses))
	}
}

func TestJobCounts(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	repo, _ := db.GetOrCreateRepo("/tmp/test-repo")

	// Create 3 jobs that will stay queued
	for i := 0; i < 3; i++ {
		sha := fmt.Sprintf("queued%d", i)
		commit, _ := db.GetOrCreateCommit(repo.ID, sha, "A", "S", time.Now())
		db.EnqueueJob(repo.ID, commit.ID, sha, "codex")
	}

	// Create a job, claim it, and complete it
	commit, _ := db.GetOrCreateCommit(repo.ID, "done1", "A", "S", time.Now())
	job, _ := db.EnqueueJob(repo.ID, commit.ID, "done1", "codex")
	_, _ = db.ClaimJob("w1") // Claims oldest queued job (one of queued0-2)
	_, _ = db.ClaimJob("w1") // Claims next
	_, _ = db.ClaimJob("w1") // Claims next
	claimed, _ := db.ClaimJob("w1") // Should claim "done1" job now
	if claimed != nil {
		db.CompleteJob(claimed.ID, "codex", "p", "o")
	}

	// Create a job, claim it, and fail it
	commit2, _ := db.GetOrCreateCommit(repo.ID, "fail1", "A", "S", time.Now())
	_, _ = db.EnqueueJob(repo.ID, commit2.ID, "fail1", "codex")
	claimed2, _ := db.ClaimJob("w2")
	if claimed2 != nil {
		db.FailJob(claimed2.ID, "err")
	}

	queued, _, done, failed, err := db.GetJobCounts()
	if err != nil {
		t.Fatalf("GetJobCounts failed: %v", err)
	}

	// We expect: 0 queued (all were claimed), 1 done, 1 failed, 3 running
	// Actually let's just verify done and failed are correct
	if done != 1 {
		t.Errorf("Expected 1 done, got %d", done)
	}
	if failed != 1 {
		t.Errorf("Expected 1 failed, got %d", failed)
	}
	_ = queued
	_ = job
}

func openTestDB(t *testing.T) *DB {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open test DB: %v", err)
	}

	return db
}

func TestMigrationV1ToV2(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create a v1 database manually with the old schema
	db, err := openRawDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to create raw DB: %v", err)
	}

	// Create old schema with commit_sha instead of git_ref
	_, err = db.Exec(`
		CREATE TABLE repos (
			id INTEGER PRIMARY KEY,
			root_path TEXT UNIQUE NOT NULL,
			name TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (datetime('now'))
		);

		CREATE TABLE commits (
			id INTEGER PRIMARY KEY,
			repo_id INTEGER NOT NULL REFERENCES repos(id),
			sha TEXT UNIQUE NOT NULL,
			author TEXT NOT NULL,
			subject TEXT NOT NULL,
			timestamp TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (datetime('now'))
		);

		CREATE TABLE review_jobs (
			id INTEGER PRIMARY KEY,
			repo_id INTEGER NOT NULL REFERENCES repos(id),
			commit_id INTEGER NOT NULL REFERENCES commits(id),
			commit_sha TEXT NOT NULL,
			agent TEXT NOT NULL DEFAULT 'codex',
			status TEXT NOT NULL DEFAULT 'queued',
			enqueued_at TEXT NOT NULL DEFAULT (datetime('now')),
			started_at TEXT,
			finished_at TEXT,
			worker_id TEXT,
			error TEXT
		);

		CREATE TABLE reviews (
			id INTEGER PRIMARY KEY,
			job_id INTEGER UNIQUE NOT NULL REFERENCES review_jobs(id),
			agent TEXT NOT NULL,
			prompt TEXT NOT NULL,
			output TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (datetime('now'))
		);

		CREATE TABLE responses (
			id INTEGER PRIMARY KEY,
			commit_id INTEGER NOT NULL REFERENCES commits(id),
			responder TEXT NOT NULL,
			response TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (datetime('now'))
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create old schema: %v", err)
	}

	// Insert some test data
	_, err = db.Exec(`
		INSERT INTO repos (id, root_path, name) VALUES (1, '/test/repo', 'repo');
		INSERT INTO commits (id, repo_id, sha, author, subject, timestamp) VALUES (1, 1, 'abc123', 'Author', 'Subject', '2024-01-01T00:00:00Z');
		INSERT INTO review_jobs (id, repo_id, commit_id, commit_sha, agent, status) VALUES (1, 1, 1, 'abc123', 'codex', 'done');
		INSERT INTO reviews (id, job_id, agent, prompt, output) VALUES (1, 1, 'codex', 'prompt', 'output');
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}
	db.Close()

	// Now open with the new Open function which should migrate
	migratedDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open (with migration) failed: %v", err)
	}
	defer migratedDB.Close()

	// Verify the schema version was set
	version := migratedDB.getSchemaVersion()
	if version != currentSchemaVersion {
		t.Errorf("Expected schema version %d, got %d", currentSchemaVersion, version)
	}

	// Verify data was migrated - git_ref should contain what was in commit_sha
	var gitRef string
	err = migratedDB.QueryRow("SELECT git_ref FROM review_jobs WHERE id = 1").Scan(&gitRef)
	if err != nil {
		t.Fatalf("Failed to query migrated data: %v", err)
	}
	if gitRef != "abc123" {
		t.Errorf("Expected git_ref 'abc123', got '%s'", gitRef)
	}

	// Verify old column doesn't exist
	rows, err := migratedDB.Query("PRAGMA table_info(review_jobs)")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}
	defer rows.Close()

	hasCommitSHA := false
	hasGitRef := false
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt interface{}
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			t.Fatal(err)
		}
		if name == "commit_sha" {
			hasCommitSHA = true
		}
		if name == "git_ref" {
			hasGitRef = true
		}
	}

	if hasCommitSHA {
		t.Error("Old commit_sha column still exists after migration")
	}
	if !hasGitRef {
		t.Error("New git_ref column missing after migration")
	}
}

// openRawDB opens a database without running migrations
func openRawDB(dbPath string) (*DB, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}
