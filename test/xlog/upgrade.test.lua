test_run = require('test_run').new()

version = test_run:get_cfg('version')
work_dir = "xlog/upgrade/"..version

test_run:cmd('create server upgrade with script="xlog/upgrade.lua", workdir="'..work_dir..'"')
test_run:cmd("start server upgrade")

test_run:switch('upgrade')

test_run:cmd(string.format("push filter '%s' to '<server_uuid>'", box.info.cluster.uuid))

--
-- Upgrade
--

box.schema.upgrade()

--
-- Migrated data
--

box.space._schema:select()
box.space._space:select()

-- Filter out options.lsn, which first appeared in 1.7.2
-- Do not use push filter, because it results in different
-- line split for 1.7.2
t = box.space._index:select()
for i, v in ipairs(t) do local opts = v[5] opts.lsn = nil t[i] = v:update{{'=', 5, opts}} end
t

box.space._user:select()
box.space._func:select()
box.space._priv:select()

box.space._vspace ~= nil
box.space._vindex ~= nil
box.space._vuser ~= nil
box.space._vpriv ~= nil

-- a test space
box.space.distro:select{}

test_run:cmd("clear filter")

test_run:switch('default')
test_run:cmd('stop server upgrade')

test_run = nil
