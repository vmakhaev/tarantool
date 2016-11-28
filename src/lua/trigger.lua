local fun = require('fun')
local log = require('log')

local table_clear = require('table.clear')

local trigger_list_mt = {
    __call = function(self, new_trigger, old_trigger)
        -- prepare, check arguments
        local tnewt, toldt = type(new_trigger), type(old_trigger)
        assert(tnewt == 'nil' or tnewt == 'function')
        assert(toldt == 'nil' or toldt == 'function')
        -- do something
        if new_trigger == nil and old_trigger == nil then
            -- list all the triggers
            return self:list()
        elseif new_trigger ~= nil and old_trigger == nil then
            -- append new trigger
            return self:append(new_trigger)
        elseif new_trigger == nil and old_trigger ~= nil then
            -- delete old trigger
            return self:delete(old_trigger)
        end
        -- if both of the arguments are functions, then we'll replace triggers
        local pos = self:find_position(old_trigger)
        if pos == nil then
            error("Trigger '%s' wasn't set", old_trigger)
        end
        old_trigger = self[pos]
        self[pos] = new_trigger
        return old_trigger
    end,
    __index = {
        append = function(self, trigger)
            table.insert(self, trigger)
        end,
        find_position = function(self, trigger)
            for pos, func in ipairs(self) do
                if trigger == func then
                    return pos
                end
            end
            return nil
        end,
        delete = function(self, trigger)
            local pos = self:find_position(trigger)
            if pos == nil then
                error(string.format("Trigger '%s' wasn't set", trigger))
            end
            return table.remove(self, pos)
        end,
        flush = function(self)
            table_clear(self)
        end,
        run = function(self, ...)
            for _, func in ipairs(self) do
                local ok, err = pcall(func, ...)
                if not ok then
                    log.info(
                        "Error, while executing '%s' trigger: %s",
                        self.name, tostring(err)
                    )
                end
            end
        end,
        list = function(self)
            local rv = {}
            for _, v in ipairs(self) do
                table.insert(rv, v)
            end
            return rv
        end
    }
}

local function trigger_list_new(name)
    return setmetatable({
        name = name
    }, trigger_list_mt)
end

return {
    new = trigger_list_new
}
