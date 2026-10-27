package store

import "context"

// ExecForTest runs sql on s's pool, for tests that set up or break state
// the store's own methods can't.
func ExecForTest(ctx context.Context, s *Store, sql string) error {
	_, err := s.pool.Exec(ctx, sql)
	return sanitize("exec for test", err)
}

// CountForTest runs a query returning one integer.
func CountForTest(ctx context.Context, s *Store, sql string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, sql).Scan(&n)
	return n, sanitize("count for test", err)
}
