#!/usr/bin/env tarantool

local tap = require('tap')
local ffi = require('ffi')
local key_def = require('key_def')

local usage_error = 'Bad params, use: key_def.new({' ..
                    '{fieldno = fieldno, type = type' ..
                    '[, is_nullable = <boolean>]' ..
                    '[, collation_id = <number>]' ..
                    '[, collation = <string>]}, ...}'

local function coll_not_found(fieldno, collation)
    if type(collation) == 'number' then
        return ('Wrong index options (field %d): ' ..
               'collation was not found by ID'):format(fieldno)
    end

    return ('Unknown collation: "%s"'):format(collation)
end

local cases = {
    -- Cases to call before box.cfg{}.
    {
        'Pass a field on an unknown type',
        parts = {{
            fieldno = 2,
            type = 'unknown',
        }},
        exp_err = 'Unknown field type: unknown',
    },
    {
        'Try to use collation_id before box.cfg{}',
        parts = {{
            fieldno = 1,
            type = 'string',
            collation_id = 2,
        }},
        exp_err = coll_not_found(1, 2),
    },
    {
        'Try to use collation before box.cfg{}',
        parts = {{
            fieldno = 1,
            type = 'string',
            collation = 'unicode_ci',
        }},
        exp_err = coll_not_found(1, 'unicode_ci'),
    },
    function()
        -- For collations.
        box.cfg{}
    end,
    -- Cases to call after box.cfg{}.
    {
        'Try to use both collation_id and collation',
        parts = {{
            fieldno = 1,
            type = 'string',
            collation_id = 2,
            collation = 'unicode_ci',
        }},
        exp_err = 'Conflicting options: collation_id and collation',
    },
    {
        'Unknown collation_id',
        parts = {{
            fieldno = 1,
            type = 'string',
            collation_id = 42,
        }},
        exp_err = coll_not_found(1, 42),
    },
    {
        'Unknown collation name',
        parts = {{
            fieldno = 1,
            type = 'string',
            collation = 'unknown',
        }},
        exp_err = 'Unknown collation: "unknown"',
    },
    {
        'Bad parts parameter type',
        parts = 1,
        exp_err = usage_error,
    },
    {
        'No parameters',
        params = {},
        exp_err = usage_error,
    },
    {
        'Two parameters',
        params = {{}, {}},
        exp_err = usage_error,
    },
    {
        'Success case; zero parts',
        parts = {},
        exp_err = nil,
    },
    {
        'Success case; one part',
        parts = {
            fieldno = 1,
            type = 'string',
        },
        exp_err = nil,
    },
}

local test = tap.test('key_def')

test:plan(#cases - 1 + 16)
for _, case in ipairs(cases) do
    if type(case) == 'function' then
        case()
    else
        local ok, res
        if case.params then
            ok, res = pcall(key_def.new, unpack(case.params))
        else
            ok, res = pcall(key_def.new, case.parts)
        end
        if case.exp_err == nil then
            ok = ok and type(res) == 'cdata' and
                ffi.istype('struct key_def', res)
            test:ok(ok, case[1])
        else
            local err = tostring(res) -- cdata -> string
            test:is_deeply({ok, err}, {false, case.exp_err}, case[1])
        end
    end
end

--
-- gh-4025: Introduce built-in function to get index key
--          from tuple.
-- gh-3398: Access to index compare function from lua.
--
tuple_a = box.tuple.new({1, 1, 22})
tuple_b = box.tuple.new({2, 1, 11})
tuple_c = box.tuple.new({3, 1, 22})
pk_parts = {{type = 'unsigned', fieldno = 1}}
sk_parts = {{type = 'number', fieldno=2}, {type='number', fieldno=3}}
pk_def = key_def.new(pk_parts)
test:is_deeply(pk_def:extract_key(tuple_a):totable(), {1}, "pk extract")
sk_def = key_def.new(sk_parts)
test:is_deeply(sk_def:extract_key(tuple_a):totable(), {1, 22}, "sk extract")

test:is(pk_def:compare(tuple_b, tuple_a), 1, "pk compare 1")
test:is(pk_def:compare(tuple_b, tuple_c), -1, "pk compare 2")
test:is(sk_def:compare(tuple_b, tuple_a), -1, "sk compare 1")
test:is(sk_def:compare(tuple_a, tuple_c), 0, "sk compare 1")

pk_sk_def = pk_def:merge(sk_def)
test:ok(pk_sk_def, "pksk merge")
test:is_deeply(pk_sk_def:extract_key(tuple_a):totable(), {1, 1, 22},
               "pksk compare 1")
test:is(pk_sk_def:compare(tuple_a, tuple_b), -1, "pksk compare 2")
sk_pk_def = sk_def:merge(pk_def)
test:ok(pk_sk_def, "skpk merge")
test:is_deeply(sk_pk_def:extract_key(tuple_a):totable(), {1, 22, 1},
               "skpk compare 1")
test:is(sk_pk_def:compare(tuple_a, tuple_b), 1, "skpk compare 1")

key = sk_def:extract_key(tuple_a)
test:is(sk_def:compare_with_key(tuple_a, key), 0,
        "sk compare_with_key - extracted tuple key")
test:is(sk_def:compare_with_key(tuple_a, {1, 22}), 0,
        "sk compare_with_key - table key")
test:is_deeply(sk_def:to_table(),
               {{type='number', fieldno=2, is_nullable=false},
                {type='number', fieldno=3, is_nullable=false}}, "sk to_table")
test:is(tostring(sk_def) == "<struct key_def &>", true, "tostring(sk_def)")

os.exit(test:check() and 0 or 1)
