#!/usr/bin/env tarantool

-- get instance name from filename (recover_missing_xlog1.lua => recover_missing_xlog1)
local INSTANCE_ID = string.match(arg[0], "%d")
local SOCKET_DIR = require('fio').cwd()
local USER = 'cluster'
local PASSWORD = 'somepassword'
local TIMEOUT = tonumber(arg[1])
local CON_TIMEOUT = arg[2] and tonumber(arg[2]) or 60.0

local function instance_uri(instance_id)
    return SOCKET_DIR..'/create_cluster'..instance_id..'.sock';
end

-- start console first
require('console').listen(os.getenv('ADMIN'))

box.cfg({
    listen = instance_uri(INSTANCE_ID);
    log_level = 7;
    replication = {
        --[[
        instance_uri(1);
        instance_uri(2);
        instance_uri(3);
        ]]--
        USER..':'..PASSWORD..'@'..instance_uri(1);
        USER..':'..PASSWORD..'@'..instance_uri(2);
        USER..':'..PASSWORD..'@'..instance_uri(3);
    };
    replication_timeout = TIMEOUT;
    replication_connect_timeout = CON_TIMEOUT;
})

box.once("bootstrap", function()
    box.schema.user.create(USER, { password = PASSWORD })
    box.schema.user.grant(USER, 'replication')
    --[[
    for i = 1, 20 do
        local u = USER .. tostring(i)
        box.schema.user.create(u, { password = PASSWORD })
        box.schema.user.grant(u, 'replication')
    end
    ]]--
    box.schema.space.create('test')
    box.space.test:create_index('primary')
    for i = 1, 500 do
        box.space.test:insert({i})
    end
end)
