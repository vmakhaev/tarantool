test_run = require('test_run').new()

fiber = require('fiber')

s = box.schema.space.create('test', {engine='vinyl'})
_ = s:create_index('primary', {unique=true, parts={1, 'unsigned'}})

function vyinfo() return box.info.vinyl().db[box.space.test.id..'/0'] end

range_count = 8
pad_size = box.cfg.vinyl.page_size / 4
keys_per_range = math.floor(box.cfg.vinyl.range_size / pad_size)
key_count = range_count * keys_per_range

-- Rewrite the space until enough ranges are created.
test_run:cmd("setopt delimiter ';'")
iter = 0
pad = string.rep('x', pad_size)
while vyinfo().range_count < range_count do
	iter = iter + 1
	for k = key_count,1,-1 do
		s:replace{k, k + iter, pad}
	end
	box.snapshot()
	fiber.sleep(0.1)
end;
function check(k1, k2)
	local result = {}
	for k = k1,k2 do
		local v = s:get(k)
		if not v or v[2] ~= k + iter then
			return false
		end
	end
	return true
end;
test_run:cmd("setopt delimiter ''");

-- Delete all tuples from all ranges except first two and last two.
-- Do not touch the first and the last range. Delete > 3/4 of tuples
-- from the second range and the next to last range.
k1 = math.floor(keys_per_range * 9 / 8)
k2 = math.ceil(key_count - keys_per_range * 9 / 8)
for k = k1,k2 do s:delete(k) end box.snapshot()

-- Wait until all interjacent ranges are coalesced.
while vyinfo().range_count > 3 do fiber.sleep(0.1) end
vyinfo().range_count == 3

-- Check the residual.
check(1, k1 - 1)
check(k2 + 1, key_count)
s:count() == key_count - (k2 - k1 + 1)

-- Empty the space.
for k = 1,key_count do s:delete(k) end box.snapshot()

-- Wait until all ranges are coalesced into one.
while vyinfo().range_count > 1 do fiber.sleep(0.1) end
vyinfo().range_count == 1

-- Ensure the space is empty.
s:select()

s:drop()
