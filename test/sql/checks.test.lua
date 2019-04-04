env = require('test_run')
test_run = env.new()
test_run:cmd("push filter ".."'\\.lua.*:[0-9]+: ' to '.lua...\"]:<line>: '")
engine = test_run:get_cfg('engine')
box.execute('pragma sql_default_engine=\''..engine..'\'')

--
-- gh-3272: Move SQL CHECK into server
--

-- Until Tarantool version 2.2 check constraints were stored in
-- space opts.
-- Make sure that now this legacy option is ignored.
opts = {checks = {{expr = 'X>5'}}}
format = {{name = 'X', type = 'unsigned'}}
t = {513, 1, 'test', 'memtx', 0, opts, format}
s = box.space._space:insert(t)
_ = box.space.test:create_index('pk')

-- Invalid expression test.
box.space._ck_constraint:insert({'CK_CONSTRAINT_01', 513, false, 'X><5', 'SQL'})
-- Unexistent space test.
box.space._ck_constraint:insert({'CK_CONSTRAINT_01', 550, false, 'X<5', 'SQL'})
-- Pass integer instead of expression.
box.space._ck_constraint:insert({'CK_CONSTRAINT_01', 513, false, 666, 'SQL'})
-- Defered CK constraints are not supported.
box.space._ck_constraint:insert({'CK_CONSTRAINT_01', 513, true, 'X<5', 'SQL'})
-- The only supperted language is SQL.
box.space._ck_constraint:insert({'CK_CONSTRAINT_01', 513, false, 'X<5', 'LUA'})

-- Check constraints LUA creation test.
box.space._ck_constraint:insert({'CK_CONSTRAINT_01', 513, false, 'X<5', 'SQL'})
box.space._ck_constraint:count({})

box.execute("INSERT INTO \"test\" VALUES(5);")
box.space._ck_constraint:replace({'CK_CONSTRAINT_01', 513, false, 'X<=5', 'SQL'})
box.execute("INSERT INTO \"test\" VALUES(5);")
box.execute("INSERT INTO \"test\" VALUES(6);")
-- Can't drop table with check constraints.
box.space.test:delete({5})
box.space.test.index.pk:drop()
box.space._space:delete({513})
box.space._ck_constraint:delete({'CK_CONSTRAINT_01', 513})
box.space._space:delete({513})

-- Create table with checks in sql.
box.execute("CREATE TABLE t1(x INTEGER CONSTRAINT ONE CHECK( x<5 ), y REAL CONSTRAINT TWO CHECK( y>x ), z INTEGER PRIMARY KEY);")
box.space._ck_constraint:count()
box.execute("INSERT INTO t1 VALUES (7, 1, 1)")
box.execute("INSERT INTO t1 VALUES (2, 1, 1)")
box.execute("INSERT INTO t1 VALUES (2, 4, 1)")
box.execute("DROP TABLE t1")

-- Test space creation rollback on spell error in ck constraint.
box.execute("CREATE TABLE first (id FLOAT PRIMARY KEY CHECK(id < 5), a INT CONSTRAINT ONE CHECK(a >< 5));")
box.space.FIRST == nil
box.space._ck_constraint:count() == 0

-- Ck constraints are disallowed for spaces having no format.
s = box.schema.create_space('test', {engine = engine})
_ = s:create_index('pk')
_ = box.space._ck_constraint:insert({'physics', s.id, false, 'X<Y', 'SQL'})
s:format({{name='X', type='integer'}, {name='Y', type='integer'}})
_ = box.space._ck_constraint:insert({'physics', s.id, false, 'X<Y', 'SQL'})
box.execute("INSERT INTO \"test\" VALUES(2, 1);")
s:format({{name='Y', type='integer'}, {name='X', type='integer'}})
box.execute("INSERT INTO \"test\" VALUES(1, 2);")
box.execute("INSERT INTO \"test\" VALUES(2, 1);")
s:truncate()
box.execute("INSERT INTO \"test\" VALUES(1, 2);")
s:format({})
s:format()
s:format({{name='Y1', type='integer'}, {name='X1', type='integer'}})
-- Ck constraint creation is forbidden for non-empty space
s:insert({2, 1})
_ = box.space._ck_constraint:insert({'conflict', s.id, false, 'X>10', 'SQL'})
s:truncate()
_ = box.space._ck_constraint:insert({'conflict', s.id, false, 'X>10', 'SQL'})
box.execute("INSERT INTO \"test\" VALUES(1, 2);")
box.execute("INSERT INTO \"test\" VALUES(11, 11);")
box.execute("INSERT INTO \"test\" VALUES(12, 11);")
s:drop()

box.execute("CREATE TABLE T2(ID INT PRIMARY KEY, CONSTRAINT CK1 CHECK(ID > 0), CONSTRAINT CK1 CHECK(ID < 0))")
box.space._ck_constraint:select()

--
-- gh-3611: Segfault on table creation with check referencing this table
--
box.execute("CREATE TABLE w2 (s1 INT PRIMARY KEY, CHECK ((SELECT COUNT(*) FROM w2) = 0));")
box.execute("DROP TABLE w2;")

--
-- gh-3653: Dissallow bindings for DDL
--
box.execute("CREATE TABLE t5(x INT PRIMARY KEY, y INT, CHECK( x*y < ? ));")

test_run:cmd("clear filter")
