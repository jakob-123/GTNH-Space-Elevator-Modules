-- MEDINA Dust Relay -- broker-managed stock targets, no local config.lua needed.
-- Replace /home/dust_telem.lua once. Edit only broker /home/config.lua thereafter.
-- IMPORTANT: Keep the ME network visible to this computer filtered/small.
-- The standard getItemsInNetwork() call reads the ENTIRE VISIBLE network.

local component     = require("component")
local serialization = require("serialization")
local event         = require("event")
local computer      = require("computer")
local term          = require("term")
local keyboard      = require("keyboard")

local PORT = 2026  -- Match config.ports.telemetry on broker (default: 2026).
local SCAN_INTERVAL = 10
local REQUEST_INTERVAL = 5
local NODE_NAME = "MEDINA-DustRelay"

assert(component.isAvailable("modem"), "Missing network card")
assert(component.isAvailable("me_controller"), "Missing ME Controller")
assert(component.isAvailable("gpu"), "Missing GPU")
local modem = component.modem
local me = component.me_controller
local gpu = component.gpu
assert(modem.isWireless and modem.isWireless(), "T2 wireless network card required")

modem.setStrength(400)
modem.open(PORT)
gpu.setResolution(80, 25)
gpu.setBackground(0x000000)

local targets = nil -- array of {name=string, threshold=number}, received from broker
local targetSet = {}
local configId = nil
local lastLines = {}
local page = 1
local lastStocks = {}
local PAGE_SIZE = 10
local nextRequest = 0
local nextScan = math.huge
local status = "Waiting for broker configuration..."

local function drawLine(y, value, color)
  local line = (value .. string.rep(" ", 78)):sub(1, 78)
  local last = lastLines[y]
  if not last or last.text ~= line or last.color ~= color then
    gpu.setForeground(color)
    gpu.set(2, y, line)
    lastLines[y] = {text=line, color=color}
  end
end

local function staticFrame()
  term.clear()
  lastLines = {}
  drawLine(2, "MEDINA DUST RELAY  |  Broker-managed targets", 0x00FF00)
  drawLine(3, string.rep("-", 76), 0x888888)
  drawLine(5, "ITEM                             STOCK / TARGET                FILL", 0x888888)
  drawLine(6, string.rep("-", 76), 0x888888)
end

local function fmt(n)
  if n >= 1000000 then return string.format("%.1fm", n / 1000000) end
  if n >= 1000 then return string.format("%.0fk", n / 1000) end
  return tostring(n)
end

local function render(stocks)
  if not targets then
    drawLine(18, status, 0xFFAA00)
    return
  end
  local list = {}
  for _, entry in ipairs(targets) do
    local stock = stocks[entry.name] or 0
    list[#list + 1] = {
      name=entry.name, stock=stock, threshold=entry.threshold,
      ratio=stock / entry.threshold
    }
  end
  table.sort(list, function(a, b)
    if a.ratio == b.ratio then return a.name < b.name end
    return a.ratio < b.ratio
  end)
  local pages = math.max(1, math.ceil(#list / PAGE_SIZE))
  if page > pages then page = pages end
  for i = 1, PAGE_SIZE do
    local entry = list[(page - 1) * PAGE_SIZE + i]
    if not entry then
      drawLine(6+i, "", 0x888888)
    else
      local pct = math.floor(entry.ratio * 100)
      local color = pct < 25 and 0xFF4444 or pct < 75 and 0xFFAA00 or 0x00FFFF
      local left = entry.name:sub(1, 29)
      drawLine(6+i, string.format("%-29s  %10s / %8s  %5d%%",
        left, fmt(entry.stock), fmt(entry.threshold), pct), color)
    end
  end
  drawLine(18, status, 0x888888)
  drawLine(20, string.format("Targets: %d  Page: %d/%d  (PgUp/PgDn to browse)",
    #targets, page, pages), 0x888888)
  drawLine(22, "Config ID: " .. configId, 0x666666)
end

-- Still one network scan: use an AE2 subnet/storage bus that exposes only
-- relevant items. This never scans a huge mainnet unless you connect one.
local function scanStock()
  local ok, items = pcall(me.getItemsInNetwork)
  if not ok or type(items) ~= "table" then
    return nil, tostring(items)
  end
  local stocks = {}
  for _, item in ipairs(items) do
    if item and type(item.label) == "string" and targetSet[item.label] then
      stocks[item.label] = (stocks[item.label] or 0) + (item.size or 0)
    end
  end
  return stocks
end

local function broadcastRequest()
  modem.broadcast(PORT, serialization.serialize({
    protocol="MEDINA_TELEMETRY", sender=NODE_NAME,
    payloadType="DUST_TARGETS_REQUEST"
  }))
end

local function acceptConfig(msg)
  if type(msg.configId) ~= "string" or type(msg.data) ~= "table" then return end
  if #msg.data > 200 then return end
  if msg.configId == configId then return end
  local newTargets, seen = {}, {}
  for _, entry in ipairs(msg.data) do
    if type(entry) ~= "table" or type(entry.name) ~= "string"
        or entry.name == "" or seen[entry.name]
        or type(entry.threshold) ~= "number" or entry.threshold <= 0 then
      status = "Invalid target list received -- waiting for retry"
      return
    end
    seen[entry.name] = true
    newTargets[#newTargets+1] = {name=entry.name, threshold=entry.threshold}
  end
  targets, targetSet, configId = newTargets, seen, msg.configId
  page = 1
  lastStocks = {}
  nextScan = 0 -- scan immediately on config changes
  status = "Config received -- checking ME network..."
  staticFrame()
  render({})
end

staticFrame()
render({})
while true do
  local now = computer.uptime()
  if not targets and now >= nextRequest then
    broadcastRequest()
    nextRequest = now + REQUEST_INTERVAL
  end
  local ev = {event.pull(0.2)}
  if ev[1] == "key_down" and targets then
    if ev[4] == keyboard.keys.pageDown or ev[4] == keyboard.keys.right then
      page = page + 1
      render(lastStocks)
    elseif ev[4] == keyboard.keys.pageUp or ev[4] == keyboard.keys.left then
      page = math.max(1, page - 1)
      render(lastStocks)
    end
  end
  if ev[1] == "modem_message" and ev[4] == PORT and type(ev[6]) == "string" then
    local ok, msg = pcall(serialization.unserialize, ev[6])
    if ok and type(msg) == "table" and msg.protocol == "MEDINA_TELEMETRY"
        and msg.payloadType == "DUST_TARGETS" then
      acceptConfig(msg)
    end
  end
  now = computer.uptime()
  if targets and now >= nextScan then
    nextScan = now + SCAN_INTERVAL
    local stocks, err = scanStock()
    if stocks then
      local payload = {}
      for _, entry in ipairs(targets) do
        payload[entry.name] = {stock=stocks[entry.name] or 0}
      end
      modem.broadcast(PORT, serialization.serialize({
        protocol="MEDINA_TELEMETRY", sender=NODE_NAME,
        payloadType="DUST_UPDATE", configId=configId, data=payload
      }))
      status = "Last successful ME scan: " .. os.date("%X")
      lastStocks = stocks
      render(stocks)
    else
      -- Fail closed: don't broadcast fabricated zeros after a failed ME query.
      status = "ME SCAN FAILED (not reporting zeros): " .. tostring(err):sub(1, 38)
      drawLine(18, status, 0xFF4444)
    end
  end
end
