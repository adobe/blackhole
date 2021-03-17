-- init character set
local charset = {}  do -- [0-9a-zA-Z]
    for c = 48, 57  do table.insert(charset, string.char(c)) end
    for c = 65, 90  do table.insert(charset, string.char(c)) end
    for c = 97, 122 do table.insert(charset, string.char(c)) end
end

-- init random seed
math.randomseed(os.clock()^5)

-- returns a random string `length` long
local function randomString(length)
    rs = ''
    for i = 1, length do
	rs = rs .. charset[math.random(1, #charset)]
    end
    return rs
end

-- Since generating random string is costly, try to 
-- pregenerate 10000 different payloads and just use
-- one of those
requests = {}
init = function() 
    for i=1, 10000 do
      requests[i] = randomString(2048)
    end
end

-- this is the real code that runs for every request
request = function ()
	wrk.method = "POST"
	-- The body below is something to create a representative json. Not actual/relevant content
	wrk.body   = requests[math.random(1, #requests)]
	wrk.headers["Content-Type"] = "application/txt"
	return wrk.format("POST", "/")
end
