package keys

import "context"

// NewRefreshingForTest is NewRefreshing with the stale-check hooks set:
// beforeRecheck runs when a refresh is about to recheck staleness under
// staleMu, afterDecide under staleMu once a check has decided, before it
// records anything. Either may be nil.
func NewRefreshingForTest(ctx context.Context, cfg Config, beforeRecheck, afterDecide func()) (*Refreshing, error) {
	return newRefreshing(ctx, cfg, staleHooks{beforeRecheck: beforeRecheck, afterDecide: afterDecide})
}

// RefreshForTest runs one refresh, as the loop does when its timer fires.
func RefreshForTest(ctx context.Context, r *Refreshing) { r.refresh(ctx) }
