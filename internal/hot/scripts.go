package hot

import "github.com/redis/go-redis/v9"

// All scripts operate on the keys of a single shard. Every key of a shard
// shares the same hash tag, so each script touches a single Redis Cluster
// slot and runs atomically. Scripts declare every key they access.
//
// Scripts read the clock with TIME on the Redis node that owns the shard.
// Accepting samples (append) and closing windows (claim) therefore use the
// same clock, which is what makes the hand-off race free: once a window is
// due, no further sample can be appended to it.
//
// claim, complete and quarantine carry the allow-oom flag (Redis >= 7.0):
// they must keep working when Redis is full, because flushing is the only
// way memory is ever freed. append has no flags and is refused as a whole
// when Redis is out of memory.

// prelude computes the server time in milliseconds.
const prelude = `
if redis.replicate_commands then redis.replicate_commands() end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
`

const allowOOM = "#!lua flags=allow-oom\n"

// appendScript appends raw samples to chunks.
//
// A new chunk starts with its fragment: 16 ASCII digits that identify this
// incarnation of the window and order it after every earlier one. The
// fragment comes from a hybrid clock per shard: the server time in
// microseconds, or one more than the last fragment of the shard if the
// clock is behind it. Fragments therefore keep increasing when the Redis
// clock steps backwards (NTP, failover to a node with a slower clock), and
// after a restore from an older snapshot, which rewinds the last fragment,
// the clock has normally moved past it.
//
// A new chunk is also registered in the due set and in the window index of
// its series, which lists the windows the series has in Redis.
//
// KEYS[1] = due set, KEYS[2] = in-flight set, KEYS[3] = shard clock,
// KEYS[4..] = (chunk key, window index) for every chunk
// ARGV[1] = maximum chunk size in bytes
// ARGV[2..] = (member, window start, due, data) for every chunk
// Returns {closed, full}: 0-based indexes of chunks whose window was closed
// and of chunks that would exceed the maximum size.
var appendScript = redis.NewScript(prelude + `
local frag = tonumber(t[1]) * 1000000 + tonumber(t[2])
local last = tonumber(redis.call('GET', KEYS[3]) or '0') or 0
if frag <= last then frag = last + 1 end
local hdr = string.format('%016.0f', frag)
local maxBytes = tonumber(ARGV[1])
local closed, full = {}, {}
local created = false
for c = 0, (#KEYS - 3) / 2 - 1 do
  local key, windows = KEYS[4 + 2 * c], KEYS[5 + 2 * c]
  local j = 2 + 4 * c
  local member, start, due, data = ARGV[j], ARGV[j + 1], tonumber(ARGV[j + 2]), ARGV[j + 3]
  if now >= due or redis.call('ZSCORE', KEYS[2], member) then
    closed[#closed + 1] = c
  else
    local len = redis.call('STRLEN', key)
    if len == 0 then len = string.len(hdr) end
    if len + string.len(data) > maxBytes then
      full[#full + 1] = c
    else
      if redis.call('EXISTS', key) == 0 then
        redis.call('SET', key, hdr)
        redis.call('ZADD', KEYS[1], due, member)
        redis.call('ZADD', windows, start, start)
        created = true
      end
      redis.call('APPEND', key, data)
    end
  end
end
if created then redis.call('SET', KEYS[3], hdr) end
return {closed, full}
`)

// claimScript leases closed chunks to a flusher. Chunks whose lease expired
// (their flusher died or was too slow) are handed out again before new ones.
// The due time of every in-flight chunk is kept in the in-flight due set,
// so that the age of the oldest chunk is always known.
//
// KEYS[1] = due set, KEYS[2] = in-flight set, KEYS[3] = in-flight due set
// ARGV[1] = maximum number of chunks, ARGV[2] = lease in milliseconds
// Returns the claimed members.
var claimScript = redis.NewScript(allowOOM + prelude + `
local limit = tonumber(ARGV[1])
local deadline = now + tonumber(ARGV[2])
local out = {}
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, limit)
for _, m in ipairs(expired) do
  redis.call('ZADD', KEYS[2], deadline, m)
  out[#out + 1] = m
end
if #out < limit then
  local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'WITHSCORES', 'LIMIT', 0, limit - #out)
  for k = 1, #due, 2 do
    local m = due[k]
    redis.call('ZREM', KEYS[1], m)
    redis.call('ZADD', KEYS[2], deadline, m)
    redis.call('ZADD', KEYS[3], due[k + 1], m)
    out[#out + 1] = m
  end
end
return out
`)

// completeScript finishes in-flight chunks.
//
// For a persisted chunk (header non-empty) the chunk is deleted only if it
// is still exactly what was persisted: same header (incarnation) and same
// length (no sample appended since it was loaded). Otherwise it is left in
// flight and re-processed after its lease expires.
//
// For a chunk that no longer exists (header empty) the in-flight entries
// are removed, so that they do not leak.
//
// A finished chunk is removed from the window index of its series.
//
// KEYS[1] = in-flight set, KEYS[2] = in-flight due set,
// KEYS[3..] = (chunk key, window index) for every chunk
// ARGV = (member, window start, header, length) for every chunk
// Returns the number of chunks finished.
var completeScript = redis.NewScript(allowOOM + `
local done = 0
for c = 0, (#KEYS - 2) / 2 - 1 do
  local key, windows = KEYS[3 + 2 * c], KEYS[4 + 2 * c]
  local j = 1 + 4 * c
  local member, start, want, size = ARGV[j], ARGV[j + 1], ARGV[j + 2], tonumber(ARGV[j + 3])
  local hdr = redis.call('GETRANGE', key, 0, 15)
  local finished = false
  if want == '' then
    finished = (hdr == '')
  elseif hdr == want and redis.call('STRLEN', key) == size then
    redis.call('DEL', key)
    finished = true
  end
  if finished then
    redis.call('ZREM', KEYS[1], member)
    redis.call('ZREM', KEYS[2], member)
    redis.call('ZREM', windows, start)
    done = done + 1
  end
end
return done
`)

// quarantineScript moves an undecodable chunk out of the way so that it
// does not block the flusher forever, while keeping it for inspection.
//
// It is fenced like completeScript: the chunk is moved only if its first 16
// bytes and its length are still those that were read. A chunk that changed
// (a newer incarnation of the window after a failover or a clock step) is
// left in flight and re-processed after its lease. A chunk that no longer
// exists is released.
//
// The quarantine key is named after the content of the chunk, so a later
// quarantine of the same window never overwrites a different chunk. Every
// quarantine key is recorded in the quarantine index with its expiry time
// (+inf without a TTL); expired entries are pruned here.
//
// KEYS[1] = in-flight set, KEYS[2] = in-flight due set, KEYS[3] = chunk key,
// KEYS[4] = quarantine key, KEYS[5] = quarantine index,
// KEYS[6] = window index of the series
// ARGV[1] = member, ARGV[2] = first 16 bytes as read, ARGV[3] = length as
// read, ARGV[4] = TTL of the quarantine key in milliseconds (0: none),
// ARGV[5] = window start
// Returns 1 if the chunk was quarantined, 0 if it changed and -1 if it no
// longer exists.
var quarantineScript = redis.NewScript(allowOOM + prelude + `
local member, want, size, ttl = ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4])
if redis.call('EXISTS', KEYS[3]) == 0 then
  redis.call('ZREM', KEYS[1], member)
  redis.call('ZREM', KEYS[2], member)
  redis.call('ZREM', KEYS[6], ARGV[5])
  return -1
end
if redis.call('GETRANGE', KEYS[3], 0, 15) ~= want or redis.call('STRLEN', KEYS[3]) ~= size then
  return 0
end
redis.call('RENAME', KEYS[3], KEYS[4])
local expiry = '+inf'
if ttl > 0 then
  redis.call('PEXPIRE', KEYS[4], ttl)
  expiry = now + ttl
end
redis.call('ZREMRANGEBYSCORE', KEYS[5], '-inf', now)
redis.call('ZADD', KEYS[5], expiry, KEYS[4])
redis.call('ZREM', KEYS[1], member)
redis.call('ZREM', KEYS[2], member)
redis.call('ZREM', KEYS[6], ARGV[5])
return 1
`)

// indexScript registers existing chunks in the window index of their
// series, for chunks written before window indexes existed. The existence
// check and the registration are atomic, so a chunk completed at the same
// time leaves no stale entry.
//
// KEYS = (chunk key, window index) for every chunk; ARGV = window starts
// Returns the number of chunks registered.
var indexScript = redis.NewScript(allowOOM + `
local n = 0
for c = 0, #KEYS / 2 - 1 do
  if redis.call('EXISTS', KEYS[1 + 2 * c]) == 1 then
    redis.call('ZADD', KEYS[2 + 2 * c], ARGV[1 + c], ARGV[1 + c])
    n = n + 1
  end
end
return n
`)
