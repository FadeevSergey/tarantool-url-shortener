local checks = require('checks')

local function init_spaces()
    local urls = box.schema.space.create('urls', {
        format = {
            {'short_url', 'string'},
            {'full_url', 'string'},
            {'call_count', 'unsigned'},
        },
        if_not_exists = true,
    })
    urls:create_index('primary', {
        parts = {'short_url'},
        if_not_exists = true,
    })

    local metrics = box.schema.space.create('metrics', {
        format = {
            {'short_url', 'string'},
	    {'count', 'unsigned'},
            {'last', 'unsigned'},
        },
        if_not_exists = true,
    })

    metrics:create_index('primary', {
        parts = {'short_url'},
        if_not_exists = true,
    })
    local domains = box.schema.space.create('domains', {
        format = {
            {'count', 'unsigned'},
            {'full_url', 'string'},
            {'domain', 'string'},
        },
        if_not_exists = true
    })    
    domains:create_index('primary', {
	parts = {'full_url'},
        if_not_exists = true,
    })
    domains:create_index('domainsk', {
	parts = {{field=3, type='string'}}, 
	unique = false,
	if_not_exists = true
    })
    domains:create_index('count', {
        parts = {{field=1, type='unsigned'}},
        unique = false,
        if_not_exists = true
    })
    -- box.schema.upgrade()
end

local function urls_add(short_url, full_url)
    box.begin()
    local s = box.space.urls:insert({
        short_url,
        full_url,
        0
    })

    local m = box.space.metrics:insert({
        short_url, 
	0,
	tonumber(os.time(os.date("!*t")))
    })

    local domain = tostring(full_url):match('^%w+://([^/]+)')     
    if not domain then
	domain = full_url
    end
    local d = box.space.domains:insert({
	full_url,
	domain,
	0
    })
    
    box.commit()
    return true
end

local function get_full_and_update(short_url)
    checks('string')
    local urls_pair = box.space.urls:get(short_url)
    
    if urls_pair == nil then
        return nil
    end

    box.space.urls:update(short_url, {
        {'+', 3, 1}
    })

    box.space.metrics:update(short_url, {
        {'+', 2, 1}, {'=', 3, os.time(os.date("!*t"))}
    })

    box.space.domains:update(tostring(urls_pair[2]), {
        {'+', 3, 1}    
    })

    return urls_pair[2]
end

local function get_requests_count(short_url_hash)
    checks('string')
    local urls_pair = box.space.metrics:get(short_url_hash)
    
    if urls_pair == nil then
        return nil
    end

    return urls_pair
end


local function get_recomend(short_url_hash, limit_)
    local urls_pair = box.space.urls:get(short_url_hash)
    if urls_pair == nil then 
	return nil
    end

    local domain_ = tostring(urls_pair[2]):match('^%w+://([^/]+)')     
    if not domain_ then
	domain_ = urls_pair[2]
    end
    -- return box.execute('select full_url, count from domains where domain = domain_ order by count;');
    return box.space.domains.index.domainsk:select({domain_}, {limit=limit_})

end

local exported_functions = {
    urls_add = urls_add,
    get_full_and_update = get_full_and_update,
    get_requests_count = get_requests_count,
    get_recomend = get_recomend,
}

local function init(opts)
    if opts.is_master then
        init_spaces()

        for name in pairs(exported_functions) do
            box.schema.func.create(name, {if_not_exists = true})
            box.schema.role.grant('public', 'execute', 'function', name, {if_not_exists = true})
        end
    end

    for name, func in pairs(exported_functions) do
        rawset(_G, name, func)
    end

    return true
end

return {
    role_name = 'storage',
    init = init,
    dependencies = {'cartridge.roles.vshard-storage'},
}
