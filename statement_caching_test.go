package pgx_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestStatementCaching tests that statements are only prepared and cached
// after they have been used once (on second use).
func TestStatementCaching(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Create a connection
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	// Helper function to count prepared statements
	countPreparedStatements := func() int {
		var count int
		err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_statements WHERE name LIKE 'stmtcache_%'").Scan(&count)
		require.NoError(t, err)
		return count
	}

	// Initially there should be no prepared statements
	initialCount := countPreparedStatements()

	// First execution - should not prepare the statement yet
	var result int
	err := conn.QueryRow(ctx, "SELECT $1::int + $2::int", 1, 2).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 3, result)
	require.Equal(t, initialCount, countPreparedStatements(), "Statement should not be prepared after first execution")

	// Second execution - should not prepare the statement yet
	err = conn.QueryRow(ctx, "SELECT $1::int + $2::int", 2, 3).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 5, result)
	require.Equal(t, initialCount, countPreparedStatements(), "Statement should not be prepared after second execution")

	// Third execution - should now prepare and cache the statement
	err = conn.QueryRow(ctx, "SELECT $1::int + $2::int", 3, 4).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 7, result)
	require.Equal(t, initialCount+1, countPreparedStatements(), "Statement should be prepared after third execution")

	// Fourth execution - should use the cached statement
	err = conn.QueryRow(ctx, "SELECT $1::int + $2::int", 4, 5).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 9, result)
	require.Equal(t, initialCount+1, countPreparedStatements(), "No additional statements should be prepared")

	// Different query - first execution
	err = conn.QueryRow(ctx, "SELECT $1::int - $2::int", 10, 5).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 5, result)
	require.Equal(t, initialCount+1, countPreparedStatements(), "Different query should not be prepared yet")

	// Different query - second execution
	err = conn.QueryRow(ctx, "SELECT $1::int - $2::int", 20, 5).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 15, result)
	require.Equal(t, initialCount+1, countPreparedStatements(), "Different query should not be prepared yet")

	// Different query - third execution, should now be prepared
	err = conn.QueryRow(ctx, "SELECT $1::int - $2::int", 30, 5).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 25, result)
	require.Equal(t, initialCount+2, countPreparedStatements(), "Different query should be prepared after third execution")
}

// TestStatementCachingDefault tests that statements are prepared after being used once (on second use).
func TestStatementCachingDefault(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Create a connection
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	// Helper function to count prepared statements
	countPreparedStatements := func() int {
		var count int
		err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_statements WHERE name LIKE 'stmtcache_%'").Scan(&count)
		require.NoError(t, err)
		return count
	}

	// Initially there should be no prepared statements
	initialCount := countPreparedStatements()

	// First execution - should not prepare the statement yet
	var result int
	err := conn.QueryRow(ctx, "SELECT $1::int + $2::int", 1, 2).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 3, result)
	require.Equal(t, initialCount, countPreparedStatements(), "Statement should not be prepared after first execution with default threshold")

	// Second execution - should now prepare the statement
	err = conn.QueryRow(ctx, "SELECT $1::int + $2::int", 3, 4).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 7, result)
	require.Equal(t, initialCount+1, countPreparedStatements(), "Statement should be prepared after second execution with default threshold")
}

// TestStatementCachingSimpleProtocol tests that statements are prepared after being used once (on second use)
// even when using simple protocol.
func TestStatementCachingSimpleProtocol(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Create a connection with simple protocol
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	// Helper function to count prepared statements
	countPreparedStatements := func() int {
		var count int
		err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_statements WHERE name LIKE 'stmtcache_%'").Scan(&count)
		require.NoError(t, err)
		return count
	}

	// Initially there should be no prepared statements
	initialCount := countPreparedStatements()

	// First execution - should not prepare the statement yet
	var result int
	err := conn.QueryRow(ctx, "SELECT $1::int + $2::int", 1, 2).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 3, result)
	require.Equal(t, initialCount, countPreparedStatements(), "Statement should not be prepared after first execution with threshold=0")

	// Second execution - should now prepare the statement
	err = conn.QueryRow(ctx, "SELECT $1::int + $2::int", 3, 4).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 7, result)
	require.Equal(t, initialCount+1, countPreparedStatements(), "Statement should be prepared after second execution with threshold=0")
}

// TestStatementCachingInvalidation tests that when a statement is invalidated,
// its tracking is reset.
func TestStatementCachingInvalidation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Create a connection
	config := mustParseConfig(t, os.Getenv("PGX_TEST_DATABASE"))
	conn := mustConnect(t, config)
	defer closeConn(t, conn)

	// Create a temporary table for this test
	_, err := conn.Exec(ctx, "CREATE TEMPORARY TABLE usage_threshold_test(id int)")
	require.NoError(t, err)

	// Helper function to count prepared statements
	countPreparedStatements := func() int {
		var count int
		err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_statements WHERE name LIKE 'stmtcache_%'").Scan(&count)
		require.NoError(t, err)
		return count
	}

	// Initially there should be no prepared statements
	initialCount := countPreparedStatements()

	// Execute the query twice - not enough to prepare it
	query := "INSERT INTO usage_threshold_test VALUES($1)"
	_, err = conn.Exec(ctx, query, 1)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, query, 2)
	require.NoError(t, err)
	require.Equal(t, initialCount, countPreparedStatements(), "Statement should not be prepared yet")

	// Invalidate all statements (this should reset tracking)
	commandTag, err := conn.Exec(ctx, "DEALLOCATE ALL")
	_ = commandTag // Ignore the command tag
	require.NoError(t, err)
	require.NoError(t, err)

	// Execute the query twice more - still not enough to prepare it
	// because the usage count was reset
	_, err = conn.Exec(ctx, query, 3)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, query, 4)
	require.NoError(t, err)
	require.Equal(t, initialCount, countPreparedStatements(), "Statement should not be prepared after invalidation and two more uses")

	// Third execution after invalidation - should now prepare it
	_, err = conn.Exec(ctx, query, 5)
	require.NoError(t, err)
	require.Equal(t, initialCount+1, countPreparedStatements(), "Statement should be prepared after third execution post-invalidation")
}
