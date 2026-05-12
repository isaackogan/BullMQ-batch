--[[
  Stage one item. If the size threshold is reached, atomically:
   - pop up to `maxBatchSize` items off the staging list
   - move them into a pending HASH entry
   - return the pending field + the items

  The caller MUST delete the pending entry (`HDEL pendingKey <field>`) after
  it has successfully enqueued the items via `queue.add`. If the caller
  crashes between this script returning and the commit, the pending entry
  becomes an orphan and is reclaimed by `batchRecover`.

  Input:
    KEYS[1]  stagingKey       -- LIST of serialized items awaiting flush
    KEYS[2]  firstTsKey       -- STRING storing the head item's arrival ts
    KEYS[3]  pendingKey       -- HASH: field = "<flusherId>:<seq>", value = JSON {ts, items}
    KEYS[4]  pendingSeqKey    -- STRING: monotonic counter for pending fields

    ARGV[1]  item              -- serialized payload to append
    ARGV[2]  maxBatchSize      -- flush threshold
    ARGV[3]  now (ms)          -- current timestamp
    ARGV[4]  flusherId         -- unique id of the calling BatchQueue instance

  Returns:
    nil                          -- staged, no flush
    { field, items[] }           -- popped into pending; caller must commit
]]

local stagingKey    = KEYS[1]
local firstTsKey    = KEYS[2]
local pendingKey    = KEYS[3]
local pendingSeqKey = KEYS[4]

local item       = ARGV[1]
local size       = tonumber(ARGV[2])
local now        = ARGV[3]
local flusherId  = ARGV[4]

local len = redis.call('RPUSH', stagingKey, item)
if len == 1 then
  redis.call('SET', firstTsKey, now)
end

if len < size then return nil end

local items = redis.call('LRANGE', stagingKey, 0, size - 1)
redis.call('LTRIM', stagingKey, size, -1)
local remaining = redis.call('LLEN', stagingKey)
if remaining == 0 then
  redis.call('DEL', firstTsKey)
else
  redis.call('SET', firstTsKey, now)
end

local seq = redis.call('INCR', pendingSeqKey)
local field = flusherId .. ':' .. seq
redis.call('HSET', pendingKey, field, cjson.encode({ts = tonumber(now), items = items}))
return {field, items}
