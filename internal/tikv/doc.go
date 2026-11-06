// Package tikv implements the persistent tier and the series index on the
// RawKV API of TiKV.
//
// Entries expire through per-key TTLs, like the Cassandra backend's rows, so
// TiKV must run with storage.enable-ttl = true and storage.api-version = 1
// (tikv.api_version v1ttl), or with api-version = 2 and enable-ttl = true
// (v2). With API v2, TiKV
// removes expired entries from disk only once they are below the GC safe
// point, which TiDB or a RawKV GC worker must advance. Every client of the
// data must use the atomic mode of RawKV, which compare-and-swap needs;
// xtsdb always does.
//
// The backend is built unless the notikv build tag is given. It is left out
// on macOS without cgo, where a dependency of the TiKV client does not
// build.
package tikv
