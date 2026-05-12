--[[
  Time-triggered flush. If either gate is met:
   - size:   LLEN >= maxBatchSize
   - time:   age >= maxBatchWaitMs AND LLEN >= minBatchSize
  the script atomically pops up to `maxBatchSize` items into the pending
  HASH and returns the pending field + items. Same commit/recovery contract
  as `batchStage`: caller MUST `HDEL pendingKey <field>` once it has called
  `queue.add` with the items.

  Input:
    KEYS[1]  stagingKey
    KEYS[2]  firstTsKey
    KEYS[3]  pendingKey
    KEYS[4]  pendingSeqKey

    ARGV[1]  maxBatchSize
    ARGV[2]  minBatchSize
    ARGV[3]  maxBatchWaitMs
    ARGV[4]  now (ms)
    ARGV[5]  flusherId

  Returns:
    nil                          -- not ready to flush
    { field, items[] }           -- popped into pending; caller must commit
]]

local stagingKey    = KEYS[1]
local firstTsKey    = KEYS[2]
local pendingKey    = KEYS[3]
local pendingSeqKey = KEYS[4]

local size       = tonumber(ARGV[1])
local minSize    = tonumber(ARGV[2])
local timeoutMs  = tonumber(ARGV[3])
local now        = tonumber(ARGV[4])
local flusherId  = ARGV[5]

local len = redis.call('LLEN', stagingKey)
if len == 0 then return nil end

local firstTs = tonumber(redis.call('GET', firstTsKey)) or now
local age = now - firstTs

local sizeReached = len >= size
local timeReached = (age >= timeoutMs) and (len >= minSize)
if not sizeReached and not timeReached then return nil end

local popCount = math.min(len, size)
local items = redis.call('LRANGE', stagingKey, 0, popCount - 1)
redis.call('LTRIM', stagingKey, popCount, -1)
local remaining = redis.call('LLEN', stagingKey)
if remaining == 0 then
  redis.call('DEL', firstTsKey)
else
  redis.call('SET', firstTsKey, tostring(now))
end

local seq = redis.call('INCR', pendingSeqKey)
local field = flusherId .. ':' .. seq
redis.call('HSET', pendingKey, field, cjson.encode({ts = now, items = items}))
return {field, items}
