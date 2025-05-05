package stmtcache

import (
	"github.com/jackc/pgx/v5/pgconn"
)

// UsageThresholdCache implements Cache with a usage threshold before caching.
// It only caches statements after they have been used a certain number of times.
type UsageThresholdCache struct {
	cache          *LRUCache
	usageThreshold int
	usageCounts    map[string]int
}

// NewUsageThresholdCache creates a new UsageThresholdCache.
// cap is the maximum size of the cache.
// threshold is the number of times a statement must be used before it is cached.
func NewUsageThresholdCache(cap int, threshold int) *UsageThresholdCache {
	return &UsageThresholdCache{
		cache:          NewLRUCache(cap),
		usageThreshold: threshold,
		usageCounts:    make(map[string]int),
	}
}

// Get returns the statement description for sql. Returns nil if not found.
func (c *UsageThresholdCache) Get(sql string) *pgconn.StatementDescription {
	// Check if the statement is already cached
	sd := c.cache.Get(sql)
	if sd != nil {
		return sd
	}

	// If not cached, increment usage count
	c.usageCounts[sql]++
	return nil
}

// Put stores sd in the cache. Put panics if sd.SQL is "".
// Put does nothing if sd.SQL already exists in the cache or
// sd.SQL has been invalidated and HandleInvalidated has not been called yet.
// Additionally, it only stores the statement if it has been used at least usageThreshold times.
func (c *UsageThresholdCache) Put(sd *pgconn.StatementDescription) {
	if sd.SQL == "" {
		panic("cannot store statement description with empty SQL")
	}

	// Only cache if the statement has been used enough times
	if c.usageCounts[sd.SQL] >= c.usageThreshold {
		c.cache.Put(sd)
	}
}

// Invalidate invalidates statement description identified by sql. Does nothing if not found.
func (c *UsageThresholdCache) Invalidate(sql string) {
	c.cache.Invalidate(sql)
	delete(c.usageCounts, sql)
}

// InvalidateAll invalidates all statement descriptions.
func (c *UsageThresholdCache) InvalidateAll() {
	c.cache.InvalidateAll()
	c.usageCounts = make(map[string]int)
}

// GetInvalidated returns a slice of all statement descriptions invalidated since the last call to RemoveInvalidated.
func (c *UsageThresholdCache) GetInvalidated() []*pgconn.StatementDescription {
	return c.cache.GetInvalidated()
}

// RemoveInvalidated removes all invalidated statement descriptions.
func (c *UsageThresholdCache) RemoveInvalidated() {
	c.cache.RemoveInvalidated()
}

// Len returns the number of cached prepared statement descriptions.
func (c *UsageThresholdCache) Len() int {
	return c.cache.Len()
}

// Cap returns the maximum number of cached prepared statement descriptions.
func (c *UsageThresholdCache) Cap() int {
	return c.cache.Cap()
}
