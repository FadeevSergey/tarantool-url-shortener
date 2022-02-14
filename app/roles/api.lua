local cartridge = require('cartridge')
local vshard = require('vshard')
local errors = require('errors')
local digest = require('digest')
local err_vshard_router = errors.new_class("Vshard routing error")


local function get_full_url(hash)
    local bucket_id = vshard.router.bucket_id(hash)
    local full_url, error = err_vshard_router:pcall(
        vshard.router.call,
        bucket_id,
        'read',
        'get_full_and_update',
        {tostring(hash)}
    )
    return full_url
end


local function generate_short_url_hash(full_url)
    return digest.md5_hex(full_url)
end

local function create_new_short_url(req)
    local full_url = req:read()
    -- generate short url
    local short_url = generate_short_url_hash(full_url)

    local bucket_id = vshard.router.bucket_id(short_url)
    local res, error = err_vshard_router:pcall(
        vshard.router.call,
        bucket_id,
        'read',
        'urls_add',
        {short_url, full_url}
    )

    local resp = req:render({json = { body = short_url }})
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.status = 200

    return resp
end


local function redirect(req)

    local short_url_hash = req:stash('hash')
    local bucket_id = vshard.router.bucket_id(short_url_hash)
    local full_url, error = err_vshard_router:pcall(
        vshard.router.call,
        bucket_id,
        'read',
        'get_full_and_update',
        {tostring(short_url_hash)}
    )

    if error then
        local resp = req:render({json = {
            info = "Internal error",
            error = error
        }})
        resp.status = 500
        return resp
    end

    if full_url == nil then
        local resp = req:render({json = { info = "Invalid short url" }})
        resp.status = 404
        return resp
    end

    if not(string.startswith(full_url, 'http://')) and not(string.startswith(full_url, 'https://')) then
        full_url = 'http://'..full_url
    end
    local resp = {status = 301, body = full_url, headers = {location = full_url}}
    return resp
end

local function redirect_count(req)
    local short_url_hash = req:stash('hash')
    local bucket_id = vshard.router.bucket_id(short_url_hash)
    local count, error = err_vshard_router:pcall(
        vshard.router.call,
        bucket_id,
        'read',
        'get_requests_count',
        {tostring(short_url_hash)}
    )

    if error then
        local resp = req:render({json = {
            info = "Internal error",
            error = error
        }})
        resp.status = 500
        return resp
    end

    if count == nil then
        local resp = req:render({json = { info = "Invalid short url" }})
        resp.status = 404
        return resp
    end
    local resp = req:render({json = { count = count[2], timestamp = count[3] }})
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.status = 200

    return resp
end

local function recomend(req)
    local short_url_hash = req:stash('hash')
    local limit = req:stash('limit')
    local bucket_id = vshard.router.bucket_id(short_url_hash)
    local result, error = err_vshard_router:pcall(
        vshard.router.call,
        bucket_id,
        'read',
        'get_recomend',
        {tostring(short_url_hash), tonumber(limit)}
    )
    print(bucket_id)
    if error then
        local resp = req:render({json = {
            info = "Internal error",
            error = error
        }})
        resp.status = 500
        return resp
    end

    if result == nil then
        local resp = req:render({json = { info = "Invalid short url" }})
        resp.status = 404
        return resp
    end

    local resp = req:render({json = result})
    resp.status = 200
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp
end


local function init(opts)
    rawset(_G, 'vshard', vshard)

    if opts.is_master then
        box.schema.user.grant('guest',
            'read,write,execute',
            'universe',
            nil, { if_not_exists = true }
        )
    end

    local httpd = assert(cartridge.service_get('httpd'), "Failed to get httpd service")
    
    if not httpd then
        return nil, err_httpd:new("http error")
    end

    httpd:route(
        { path = '/set', method = 'POST', public = true },
        create_new_short_url
    )

    httpd:route(
        { path = '/:hash', method = 'GET', public = true },
        redirect
    )

    httpd:route(
        { path = '/count/:hash', method = 'GET', public = true },
        redirect_count
    )

     httpd:route(
        { path = '/recomend/:hash/:limit', method = 'GET', public = true },
        recomend
    )
    return true
end


return {
    role_name = 'api',
    init = init,
    dependencies = {'cartridge.roles.vshard-router'},
}
