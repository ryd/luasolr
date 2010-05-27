local http   = require("socket.http")
local json   = require("json")
local table  = require("table")
local ltn12  = require("ltn12")
local string = require("string")
local base   = _G
module("solr")

config       = config or {}
config.host  = "localhost"
config.port  = 8983
config.debug = false
config.base  = "solr/"

local solr_request

function solr_request(request)
    -- ensure methode
    if request.methode == nil then
        request.methode = 'GET'
    end

    local t = {}
    -- request parameter
    local param = {
        url     = string.format("http://%s:%d/%s%s", config.host, config.port, config.base, request.path),
        method  = request.methode,
        sink    = ltn12.sink.table(t),
        headers = { 
            ["Connection"]          = 'close',
        }
    }

    -- POST body
    if request.body ~= nil then
        param.source = ltn12.source.string(request.body)
        param.headers["Content-Length"] = string.len(request.body)
        param.headers["Content-Type"]   = request.contenttype or "plain/text"
    end

    -- send request
    local response, code = http.request(param)

    -- debug
    if config.debug then
        base.print('#### ' .. request.methode .. ' ' .. request.path .. ' ####')
        base.print('code = ' .. code)
        base.print(table.concat(t))
        if request.body ~= nil then
            base.print('body = ' .. request.body)
        end
    end

    -- ensure right reponse
    if request.code ~= nil then
        base.assert(code == request.code, 'unexpected return code - ' .. code)
    end

    return json.decode(table.concat(t)), nil
end

function join(t)
    if t == nil or t.length == 0 then
        return ""
    end

    local len = #t

    if len == 1 then
        return t[1]
    end

    local res = t[1]

    for i = 2, len do
        res = res .. "&" .. t[i]
    end

    return res
end


function query(param)
    local query = {}
    param = param or {}
    
    if param.query ~= nil then
        table.insert(query, "q=" .. url_encode(param.query))
    else
        return nil
    end

    if param.start ~= nil then
        table.insert(query, "start=" .. param.start)
    end

    if param.filter ~= nil then
        table.insert(query, "fq=" .. param.filter)
    end

    if param.rows ~= nil then
        table.insert(query, "rows=" .. param.rows)
    else
        table.insert(query, "rows=10")
    end

    if param.fields ~= nil then
        table.insert(query, "fl=" .. param.fields)
    else
        table.insert(query, "fl=*,score")
    end

    if param.facet ~= nil then
        table.insert(query, "facet=true")
        solr_facet_fields(query, param.facet.fields)

        -- limit
    end

    -- make it json
    table.insert(query, "wt=json")

    local res, err = solr_request({ path = 'select?' .. join(query), code = 200 })

    if err ~= nil then
        base.print("error - " .. err)
    --    return nil, err
    end

    return res.response, nil, res.responseHeader
end

function solr_facet_fields(query, fields)
    if fields == nil then
        return
    end

    if base.type(fields) == 'string' then
        table.insert(query, 'facet.field=' .. fields)
        return
    end

    if base.type(fields) == 'table' then
        for i,v in base.ipairs(fields) do
            solr_facet_fields(query, v)
        end
    end
end

function post(param)
    local query = {}
    table.insert(query, "commit=true")
    table.insert(query, "wt=json")

    local res, err = solr_request({ 
        path = 'update?' .. join(query), 
        --code = 200 ,
        methode = 'POST',
        contenttype = 'text/xml',
        body = '<add>' .. table2xml(param.data) .. '</add>',
    })
    return res, err
end

function table2xml(data)
    if data == nil then
        return ''
    end

    local result = ''

    if base.type(data) == 'table' then
        for k, v in base.pairs(data) do
            if v == nil then v = '' end

            if base.type(k) == 'number' and base.type(v) == 'table' then
                result = result .. "<doc>\n" .. table2xml(v) .. "</doc>\n"
            elseif base.type(k) == 'string' then
                result = result .. '<field name="' .. k .. '">' .. v .. '</field>' .. "\n"
            end
        end
    end

    return result
end

function url_encode(str)
    if (str) then
        str = string.gsub (str, "\n", "\r\n")
        str = string.gsub (str, "([^%w ])", function (c) return string.format ("%%%02X", string.byte(c)) end)
        str = string.gsub (str, " ", "+")
    end
    return str    
end
