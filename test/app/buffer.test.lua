test_run = require('test_run').new()
buffer = require('buffer')
ffi = require('ffi')

-- Scalar.
scalar = buffer.scalar
scalar.u16 = 100
u16 = ffi.new('uint16_t[1]')
ffi.copy(u16, scalar, 2)
u16[0]

u16[0] = 200
ffi.copy(scalar, u16, 2)
scalar.u16

-- Alignment.
_ = buffer.static_alloc('char') -- This makes buffer pos unaligned.
p = buffer.static_alloc('int')
ffi.cast('int', p) % ffi.alignof('int') == 0 -- But next alloc is aligned.

-- Able to allocate bigger than static buffer - such allocations
-- are on the heap.
type(buffer.static_alloc('char', 13000))
