local ffi = require('ffi')
local key_def = require('key_def')
local key_def_t = ffi.typeof('struct key_def')

local methods = {
    ['extract_key'] = key_def.internal.extract_key,
    ['compare'] = key_def.internal.compare,
    ['compare_with_key'] = key_def.internal.compare_with_key,
    ['merge'] = key_def.internal.merge,
    ['to_table'] = key_def.internal.to_table,
    ['__serialize'] = key_def.internal.to_table,
}

ffi.metatype(key_def_t, {
    __index = function(self, key)
        return methods[key]
    end,
    __tostring = function(key_def) return "<struct key_def &>" end,
})
