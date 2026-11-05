package hot

import "github.com/redis/go-redis/v9"

// All scripts operate on the keys of a single shard. Every key of a shard
// shares the same hash tag, so each script touches a single Redis Cluster
// slot and runs atomically.
//
// Scripts read the clock with TIME on the Redis node that owns the shard.
// Accepting samples (append) and closing windows (claim) therefore use the
// same clock, which is what makes the hand-off race free: once a window is
// due, no further sample can be appended to it.

// prelude enables effect replication on Redis < 7 (TIME is non-deterministic)
// and computes the server time in milliseconds and a 16-digit microsecond
// stamp used as the chunk header.
const prelude = `
if redis.replicate_commands then redis.replicate_commands() end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
`

// appendScript appends raw samples to chunks.
//
// KEYS[1] = due set, KEYS[2] = in-flight set, KEYS[3..] = chunk keys
// ARGV    = (member, due, data) for every chunk key
// Returns the 0-based indexes of chunks whose window was closed.
var appendScript = redis.NewScript(prelude + `
local hdr = string.rep('0', 10 - string.len(t[1])) .. t[1] .. string.rep('0', 6 - string.len(t[2])) .. t[2]
local rejected = {}
for i = 3, #KEYS do
  local j = (i - 3) * 3
  local member = ARGV[j + 1]
  local due = tonumber(ARGV[j + 2])
  if now >= due or redis.call('ZSCORE', KEYS[2], member) then
    rejected[#rejected + 1] = i - 3
  else
    if redis.call('EXISTS', KEYS[i]) == 0 then
      redis.call('SET', KEYS[i], hdr)
      redis.call('ZADD', KEYS[1], due, member)
    end
    redis.call('APPEND', KEYS[i], ARGV[j + 3])
  end
end
return rejected
`)

// claimScript leases closed chunks to a flusher. Chunks whose lease expired
// (their flusher died) are handed out again before new ones.
//
// KEYS[1] = due set, KEYS[2] = in-flight set
// ARGV[1] = maximum number of chunks, ARGV[2] = lease in milliseconds
// Returns the claimed members.
var claimScript = redis.NewScript(prelude + `
local limit = tonumber(ARGV[1])
local deadline = now + tonumber(ARGV[2])
local out = {}
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, limit)
for _, m in ipairs(expired) do
  redis.call('ZADD', KEYS[2], deadline, m)
  out[#out + 1] = m
end
if #out < limit then
  local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit - #out)
  for _, m in ipairs(due) do
    redis.call('ZREM', KEYS[1], m)
    redis.call('ZADD', KEYS[2], deadline, m)
    out[#out + 1] = m
  end
end
return out
`)

// completeScript deletes persisted chunks. A chunk is deleted only if its
// header still matches the persisted fragment, so a flusher whose lease
// expired can never delete a newer incarnation of the same chunk.
//
// KEYS[1] = in-flight set, KEYS[2..] = chunk keys
// ARGV    = (member, header) for every chunk key
// Returns the number of chunks completed.
var completeScript = redis.NewScript(`
local done = 0
for i = 2, #KEYS do
  local j = (i - 2) * 2
  local hdr = redis.call('GETRANGE', KEYS[i], 0, 15)
  if hdr == ARGV[j + 2] or hdr == '' then
    if hdr ~= '' then redis.call('DEL', KEYS[i]) end
    redis.call('ZREM', KEYS[1], ARGV[j + 1])
    done = done + 1
  end
end
return done
`)

// quarantineScript moves an undecodable chunk out of the way so that it
// does not block the flusher forever, while keeping it for inspection.
//
// KEYS[1] = in-flight set, KEYS[2] = chunk key, KEYS[3] = quarantine key
// ARGV[1] = member
var quarantineScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then
  redis.call('RENAME', KEYS[2], KEYS[3])
end
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`)
