#!/usr/bin/env tarantool

box.cfg{
    logger = "tarantool.log"
}

local trigger = require('internal.trigger')
local test = require('tap').test('trigger')

test:plan(3)

local trigger_list = trigger.new("sweet trigger")
test:ok(trigger_list ~= nil, "test that trigger list is created")

test:test("simple trigger test", function(test)
    test:plan(8)

    local cnt = 0
    local function trigger_cnt() cnt = cnt + 1 end

    -- Append first trigger
    trigger_list:append(trigger_cnt)
    trigger_list:run()
    test:is(cnt, 1, "check first run")
    -- Append second trigger
    trigger_list:append(trigger_cnt)
    trigger_list:run()
    test:is(cnt, 3, "check first run")
    -- Delete both triggers
    local list_copy = trigger_list:list()
    test:is(#list_copy, 2, "trigger:list() count")
    table.remove(list_copy)
    test:is(#trigger_list:list(), 2, "check that we've returned copy")

    test:is(trigger_list:delete(trigger_cnt), trigger_cnt, "pop trigger")
    trigger_list:run()
    test:is(cnt, 4, "check third run")
    test:is(trigger_list:delete(trigger_cnt), trigger_cnt, "pop trigger")
    trigger_list:run()


    -- Check that we've failed to delete trigger
    local stat, err = pcall(trigger_list.delete, trigger_list, trigger_cnt)
    test:ok(string.find(err, "wasn't set"), "check error")
end)

test:test("errored trigger test", function(test)
    test:plan(6)

    local cnt = 0
    local function trigger_cnt() cnt = cnt + 1 end
    local function trigger_errored() error("test error") end

    test:is(#trigger_list:list(), 0, "check for empty triggers")

    -- Append first trigger
    trigger_list:append(trigger_cnt)
    trigger_list:run()
    test:is(cnt, 1, "check simple trigger")
    -- Append errored trigger
    trigger_list:append(trigger_errored)
    trigger_list:run()
    test:is(cnt, 2, "check simple+error trigger")
    -- Flush triggers
    trigger_list:flush()
    test:is(#trigger_list:list(), 0, "successfull flush")
    -- Append first trigger
    trigger_list:append(trigger_errored)
    trigger_list:run()
    test:is(cnt, 2, "check error trigger")
    -- Append errored trigger
    trigger_list:append(trigger_cnt)
    trigger_list:run()
    test:is(cnt, 3, "check error+simple trigger")
end)

os.exit(test:check() == true and 0 or -1)
