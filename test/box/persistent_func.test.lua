env = require('test_run')
test_run = env.new()

--
-- gh-4182: Add persistent LUA functions.
--
box.schema.user.grant('guest', 'execute', 'universe')
net = require('net.box')
conn = net.connect(box.cfg.listen)

-- Test valid function.
test_run:cmd("setopt delimiter ';'")
body = [[function(tuple)
	if type(tuple.address) ~= 'string' then return nil, 'Invalid field type' end
	local t = tuple.address:lower():split()
	for k,v in pairs(t) do t[k] = {v} end
	return t
end
]]
test_run:cmd("setopt delimiter ''");
box.schema.func.create('test', {body = body, language = "C"})
box.schema.func.create('test', {body = body})
box.schema.func.exists('test')
conn:call("test", {{address = "Moscow Dolgoprudny"}})
box.schema.func.create('test2', {body = body, opts = {is_deterministic = true}})
box.schema.func.create('test3', {body = body, opts = {is_deterministic = true, extra = true}})

-- Test that monkey-patch attack is not possible.
test_run:cmd("setopt delimiter ';'")
body_monkey = [[function(tuple)
	math.abs = math.log
	return tuple
end
]]
test_run:cmd("setopt delimiter ''");
box.schema.func.create('body_monkey', {body = body_monkey})
conn:call("body_monkey", {{address = "Moscow Dolgoprudny"}})
math.abs(-666.666)

-- Test taht 'require' is forbidden.
test_run:cmd("setopt delimiter ';'")
body_bad1 = [[function(tuple)
	local json = require('json')
	return json.encode(tuple)
end
]]
test_run:cmd("setopt delimiter ''");
box.schema.func.create('json_serializer', {body = body_bad1})
conn:call("json_serializer", {{address = "Moscow Dolgoprudny"}})

-- Test function with spell error - case 1.
test_run:cmd("setopt delimiter ';'")
body_bad2 = [[function(tuple)
	ret tuple
end
]]
test_run:cmd("setopt delimiter ''");
box.schema.func.create('body_bad2', {body = body_bad2})

-- Test function with spell error - case 2.
test_run:cmd("setopt delimiter ';'")
body_bad3 = [[func(tuple)
	return tuple
end
]]
test_run:cmd("setopt delimiter ''");
box.schema.func.create('body_bad3', {body = body_bad3})
conn:call("body_bad3", {{address = "Moscow Dolgoprudny"}})

conn:close()
-- Restart server.
test_run:cmd("restart server default")
net = require('net.box')
test_run = require('test_run').new()
conn = net.connect(box.cfg.listen)
conn:call("test", {{address = "Moscow Dolgoprudny"}})
conn:close()
box.schema.func.drop('test')
box.schema.func.exists('test')
box.schema.func.drop('body_monkey')
box.schema.func.drop('json_serializer')
box.schema.func.drop('test2')
box.schema.user.revoke('guest', 'execute', 'universe')
