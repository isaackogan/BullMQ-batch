--[[
  Scan the pending HASH for entries older than the recovery threshold. For
  each orphan: push its items back onto the staging list and delete the
  pending entry. Items rejoin the normal batching flow and will be flushed
  again by `batchStage` / `batchTryFlush`.

  Atomicity: the whole scan + push-back + delete sequence runs inside one
  Lua call, so two BatchQueue instances racing to recover the same pending
  entry cannot produce duplicates — the second `HDEL` is a no-op.

  Note that "recovery" here means the items are at-least-once: if a flusher
  hung mid-`queue.add` long enough for recovery to fire, the items may end
  up in a future batch in addition to the original (eventually-succeeded)
  one. Set the threshold well above expected `queue.add` latency.

  Input:
    KEYS[1]  pendingKey
    KEYS[2]  stagingKey
    KEYS[3]  firstTsKey

    ARGV[1]  thresholdMs        -- pending entries older than this are orphans
    ARGV[2]  now (ms)

  Returns: integer total number of items re-staged.
]]

local pendingKey  = KEYS[1]
local stagingKey  = KEYS[2]
local firstTsKey  = KEYS[3]

local thresholdMs = tonumber(ARGV[1])
local now         = tonumber(ARGV[2])

local fields = redis.call('HKEYS', pendingKey)
local recovered = 0

for _, field in ipairs(fields) do
  local raw = redis.call('HGET', pendingKey, field)
  if raw then
    local entry = cjson.decode(raw)
    if (now - entry.ts) >= thresholdMs then
      local wasEmpty = redis.call('LLEN', stagingKey) == 0
      for _, item in ipairs(entry.items) do
        redis.call('RPUSH', stagingKey, item)
      end
      if wasEmpty and #entry.items > 0 then
        redis.call('SET', firstTsKey, tostring(now))
      end
      redis.call('HDEL', pendingKey, field)
      recovered = recovered + #entry.items
    end
  end
end

return recovered
