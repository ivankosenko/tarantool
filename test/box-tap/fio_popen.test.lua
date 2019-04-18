#!/usr/bin/env tarantool

local tap = require('tap')
local fio = require('fio')
local test = tap.test()

test:plan(2 * 3)

box.cfg{}

local function start_app(script_path)
    local fh = fio.popen(script_path, "r")

    if fh == nil then
        local err = errno.strerror()
        error(string.format("Failed to run app: %s, error: %s", script_path, err))
        return nil
    else
        return fh
    end
end

function read_stdout(fh)
    local ss = ""

    local s,src = fh:read(64)

    while s ~= nil and s ~= "" do
        if src == nil then
            src = -1
            print("the source is undefined")
        else
            ss = ss .. string.format("[%d] %s", src, s)
        end

        s,src = fh:read(64)
    end

    return ss
end

local build_path = os.getenv("BUILDDIR")
local app_path = fio.pathjoin(build_path, 'test/box-tap/fio_popen_test1.sh')

local the_app = start_app(app_path)
test:isnt(the_app, nil, "starting a child process 1")

local app_output = read_stdout(the_app)
test:isnt(app_output, nil, "response from child process 1")

local expected_output = "[1] 1\n[1] 2\n[1] 3\n[1] 4\n[1] 5\n[1] 6\n[1] 7\n[1] 8\n[1] 9\n[1] 10\n"

test:is(app_output, expected_output, "compare response 1")

the_app:close()

-- Try to get STDERR output
the_app = start_app("/fufel/fufel.sh")
test:isnt(the_app, nil, "starting a not existing process")

app_output = read_stdout(the_app)
test:isnt(app_output, nil, "response from child process 2")

expected_output = "/fufel/fufel.sh: not found"
test:like(app_output, expected_output, "compare response 2")

the_app:close()


test:check()
os.exit(0)

