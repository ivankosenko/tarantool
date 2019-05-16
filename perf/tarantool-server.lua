local console = require('console')
console.listen("/tmp/tarantool-server.sock")

box.cfg {
    pid_file   		= "./tarantool-server.pid",
    log        		= "./tarantool-server.log",
    listen 		= 3301,
    memtx_memory 	= 2000000000,
    background 		= true,
    checkpoint_interval = 0,
}


function try(f, catch_f)
    local status, exception = pcall(f)
    if not status then
        catch_f(exception)
    end
end

try(function()
    box.schema.user.grant('guest', 'create,read,write,execute', 'universe')
end, function(e)
    print(e)
end)

