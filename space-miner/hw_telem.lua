-- =============================================================================
-- Node ID: MEDINA-HWRelay
-- File: hw_telem.lua
-- Purpose: Scans Space Elevator staging ME network for drone and drill
--          consumable availability; broadcasts stock data to the broker.
-- MEDINA RESERVATION FIX:
--   * Broadcast an explicit zero for EVERY tracked drone/drill.
--   * Do not broadcast a fabricated empty inventory if the AE2 scan fails.
-- =============================================================================
local component     = require("component")
local serialization = require("serialization")
local term          = require("term")
local event         = require("event")

if not component.isAvailable("modem") then error("Missing network card.") end
if not component.isAvailable("gpu") then error("Requires GPU.") end

local modem = component.modem
if not modem.isWireless or not modem.isWireless() then
  error("Node requires a T2 Wireless Network Card.")
end

local me = nil
for addr, name in component.list("me_controller") do
  me = component.proxy(addr)
  break
end
if not me then error("Missing ME Controller (needed to read network inventory).") end

local gpu      = component.gpu
local nodeName = "MEDINA-HWRelay"
local droneKeys = {"max","uxv","umv","uiv","uev","uhv","uv","zpm","luv","iv","ev","hv","mv","lv"}
local droneNames = {
  max="Mining Drone MK-XIV", uxv="Mining Drone MK-XIII", umv="Mining Drone MK-XII",
  uiv="Mining Drone MK-XI", uev="Mining Drone MK-X", uhv="Mining Drone MK-IX",
  uv="Mining Drone MK-VIII", zpm="Mining Drone MK-VII", luv="Mining Drone MK-VI",
  iv="Mining Drone MK-V", ev="Mining Drone MK-IV", hv="Mining Drone MK-III",
  mv="Mining Drone MK-II", lv="Mining Drone MK-I"
}
local droneVoltages = {
  max="MAX", uxv="UXV", umv="UMV", uiv="UIV", uev="UEV", uhv="UHV",
  uv="UV", zpm="ZPM", luv="LuV", iv="IV", ev="EV", hv="HV",
  mv="MV", lv="LV"
}

modem.setStrength(400)
modem.open(2026)
gpu.setResolution(80, 25)
local drillLookup = {
  ["Steel Drill Tip"]              = "steel",
  ["Steel Rod"]                    = "steel",
  ["Titanium Drill Tip"]           = "titanium",
  ["Titanium Rod"]                 = "titanium",
  ["Tungstensteel Drill Tip"]      = "tungstensteel",
  ["Tungstensteel Rod"]            = "tungstensteel",
  ["Naquadah Drill Tip"]           = "naquadah",
  ["Naquadah Rod"]                 = "naquadah",
  ["Naquadah Alloy Drill Tip"]     = "naquadahAlloy",
  ["Naquadah Alloy Rod"]           = "naquadahAlloy",
  ["Neutronium Drill Tip"]         = "neutronium",
  ["Neutronium Rod"]               = "neutronium",
  ["Cosmic Neutronium Drill Tip"]  = "cosmicNeutronium",
  ["Cosmic Neutronium Rod"]        = "cosmicNeutronium",
  ["Infinity Drill Tip"]           = "infinity",
  ["Infinity Rod"]                 = "infinity",
  ["Transcendent Metal Drill Tip"] = "transcendentMetal",
  ["Transcendent Metal Rod"]       = "transcendentMetal"
}
local drillKeyOrder = {
  "steel","titanium","tungstensteel","naquadah",
  "naquadahAlloy","neutronium","cosmicNeutronium","infinity","transcendentMetal"
}
local drillDisplayNames = {
  steel="Steel", titanium="Titanium", tungstensteel="Tungstensteel",
  naquadah="Naquadah", naquadahAlloy="Naquadah Alloy", neutronium="Neutronium",
  cosmicNeutronium="Cosmic Neutronium", infinity="Infinity", transcendentMetal="Transcendent Metal"
}

local function drawStaticFrame()
  term.clear()
  gpu.setForeground(0x00FF00)
  print("================================================================================")
  print(" MEDINA RELAY NETWORK  |  NODE: " .. nodeName)
  print("================================================================================")
  gpu.setForeground(0xFFFFFF)
  term.setCursor(2, 5)  io.write("DRONE FLEET STATUS")
  term.setCursor(40, 5) io.write("DRILL KIT AVAILABILITY")
  term.setCursor(2, 6)  io.write(string.rep("-", 76))
  gpu.setForeground(0x555555)
  term.setCursor(2, 22) io.write(string.rep("=", 76))
  term.setCursor(2, 23) io.write("  Wireless Signal Range: " .. tostring(modem.getStrength()) .. " blocks")
  term.setCursor(2, 24) io.write("  Network Port: 2026")
end

-- A failed AE2 scan is NOT a valid report of zero stock.
local function scanAssets()
  local assets = { drones={}, drillTips={}, drillRods={} }
  local success, itemList = pcall(me.getItemsInNetwork)
  if not success or type(itemList) ~= "table" then
    return nil, tostring(itemList)
  end
  for _, item in ipairs(itemList) do
    if item and item.label then
      if string.find(item.label, "Mining Drone", 1, true) then
        assets.drones[item.label] = (assets.drones[item.label] or 0) + (item.size or 0)
      elseif drillLookup[item.label] then
        local key = drillLookup[item.label]
        if string.find(item.label, "Drill Tip", 1, true) then
          assets.drillTips[key] = (assets.drillTips[key] or 0) + (item.size or 0)
        elseif string.find(item.label, "Rod", 1, true) then
          assets.drillRods[key] = (assets.drillRods[key] or 0) + (item.size or 0)
        end
      end
    end
  end
  return assets
end

local function updateDashboard(assets)
  local totalDrones = 0
  for i, key in ipairs(droneKeys) do
    local label = droneNames[key]
    local count = assets.drones[label] or 0
    totalDrones = totalDrones + count
    local row = 6 + i
    term.setCursor(2, row)
    gpu.fill(2, row, 36, 1, " ")
    gpu.setForeground(count > 0 and 0x00FFFF or 0x555555)
    local voltage = droneVoltages[key]
    io.write(string.format("  %-14s [%s]: %d", string.sub(label, 14), voltage, count))
  end
  for i, key in ipairs(drillKeyOrder) do
    local tips = assets.drillTips[key] or 0
    local rods = assets.drillRods[key] or 0
    local kits = math.min(tips, rods)
    local displayName = drillDisplayNames[key]
    local row = 6 + i
    term.setCursor(40, row)
    gpu.fill(40, row, 38, 1, " ")
    if totalDrones > 0 then
      gpu.setForeground(kits > 0 and 0xFF00FF or 0x555555)
      io.write(string.format("  %-15s: %d (%d|%d)", displayName, kits, tips, rods))
    else
      gpu.setForeground(0x333333)
      io.write("  [ NO FLEET — MASKED ]")
    end
  end
  gpu.setForeground(0x555555)
  term.setCursor(55, 2)
  io.write("LAST_SYNC: " .. os.date("%X"))
end

local function buildPayload(assets)
  local payload = { drones={}, drills={} }
  for _, key in ipairs(droneKeys) do
    -- Explicit zeros are vital: the broker needs to see the transition 1 -> 0.
    payload.drones[key] = assets.drones[droneNames[key]] or 0
  end
  for _, key in ipairs(drillKeyOrder) do
    local tips = assets.drillTips[key] or 0
    local rods = assets.drillRods[key] or 0
    payload.drills[key] = { kits=math.min(tips, rods), tips=tips, rods=rods }
  end
  return payload
end

modem.open(2025) -- Query port for on-demand inventory requests
local lastAssets = nil
local lastScanError = nil

while true do
  drawStaticFrame()
  local assets, err = scanAssets()
  if assets then
    lastAssets = assets
    if lastScanError then
      gpu.setBackground(0x000000)
      gpu.fill(2, 25, 76, 1, " ")
    end
    lastScanError = nil
    updateDashboard(lastAssets)
    modem.broadcast(2026, serialization.serialize({
      protocol = "MEDINA_TELEMETRY",
      sender = nodeName,
      payloadType = "HW_UPDATE",
      data = buildPayload(lastAssets)
    }))
  else
    -- No broadcast on failure: the broker's HW timeout will block new jobs
    -- rather than trust invented zeros. Retain the last known dashboard.
    if err ~= lastScanError then
      lastScanError = err
      -- Avoid printing into the dashboard every ten seconds.
      gpu.setForeground(0xFF4444)
      term.setCursor(2, 25)
      gpu.fill(2, 25, 76, 1, " ")
      io.write("AE2 scan failed; telemetry paused")
    end
  end

  local ev = { event.pull(10, "key_down", "modem_message") }
  if ev[1] == "key_down" and ev[3] == 3 then
    term.clear()
    os.exit()
  elseif ev[1] == "modem_message" then
    local _, _, senderAddr, port, _, rawMsg = table.unpack(ev)
    if port == 2025 and lastAssets then
      local ok, msg = pcall(serialization.unserialize, rawMsg)
      if ok and msg and msg.protocol == "MEDINA_COMMAND" and msg.payloadType == "HW_QUERY" then
        modem.send(senderAddr, 2025, serialization.serialize({
          protocol = "MEDINA_TELEMETRY",
          sender = nodeName,
          payloadType = "HW_QUERY_RESPONSE",
          data = buildPayload(lastAssets)
        }))
      end
    end
  end
end
