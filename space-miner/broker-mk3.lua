-- MEDINA_SAFE_STOP_PATCH: UI cache + sorted dispatch + F10 safe stop
-- MEDINA DRONE FIX: account only for allocations not reflected in HW telemetry
-- MEDINA THREE_STACKS: 192 tips + 192 rods per job; resource reservations
-- MEDINA REPAIR: compatible loader + visible module error details
-- MEDINA_LOADER: restored slot reads, timed stages, and extra consumable stacks
-- MEDINA_DUST_CONFIG: broker-owned dust configuration; paged dust node compatible
-- =============================================================================
-- MEDINA BROKER MK3  (v1.5)
-- Consolidated broker: telemetry aggregation + dispatch + cooperative consumable
-- loading, all on one computer.
--
-- WHAT'S NEW vs MK2:
--   - Loads run as cooperative TASKS (see scheduler.lua + loader.lua), so all six
--     modules load concurrently and the UI / telemetry NEVER freeze.
--   - The 10-second per-module stagger is GONE. Loads are self-pacing: each one
--     confirms its database fingerprints by read-back (db.get) instead of sleeping
--     a fixed guess. Fast when the server is fast, patient when it lags.
--   - One clock for everything (computer.uptime, via the scheduler).
--
-- Hardware (unchanged from MK2):
--   - T2 Wireless Network Card (telemetry on config.ports.telemetry)
--   - GPU + screen for UI
--   - ONE OC Database (slots partitioned per module: M1->1-3, M2->4-6, ...)
--   - Per-module: Adapter (module controller), Adapter (ME interface), Transposer
--
-- Requires: /home/scheduler.lua, /home/loader.lua,
--           /home/job_node_config.lua, /home/config.lua, /home/logger.lua
-- =============================================================================

local component = require("component")
local serial    = require("serialization")
local event     = require("event")
local keyboard  = require("keyboard")
local term      = require("term")
local fs        = require("filesystem")
local computer  = require("computer")

local config = dofile("/home/config.lua")
local sched  = dofile("/home/scheduler.lua")
local loader = dofile("/home/loader.lua")

local loggingModule = dofile("/home/logger.lua")
assert(loggingModule and loggingModule.createLogger, "logger.lua not loaded")
local logger = loggingModule.createLogger("broker-mk3")
local getUnixTime = loggingModule.getCurrentTimestamp

logger:info("========== BROKER-MK3 (v1.5 REPAIR) STARTUP ==========")

-- Surface task crashes in the log instead of swallowing them.
local taskErrors = {}
sched.onError = function(name, err)
  taskErrors[name] = tostring(err)
  logger:error("[TASK] " .. tostring(name) .. " crashed: " .. tostring(err))
end

-- =============================================================================
-- HARDWARE VALIDATION
-- =============================================================================

if not component.isAvailable("modem") then error("Missing network card.") end
local modem = component.modem
if not modem.isWireless or not modem.isWireless() then
  error("Requires a T2 Wireless Network Card.")
end
modem.setStrength(400)
modem.open(config.ports.telemetry)
logger:info("Modem listening on port " .. config.ports.telemetry)

local gpu = component.isAvailable("gpu") and component.gpu or nil

-- =============================================================================
-- LOAD MODULE CONFIG
-- =============================================================================

local CONFIG_PATH = "/home/job_node_config.lua"
assert(fs.exists(CONFIG_PATH), "Missing " .. CONFIG_PATH)
local nodeConf = dofile(CONFIG_PATH)
local nodeId = assert(nodeConf.nodeId, "nodeId missing from job_node_config.lua")

local function getProxy(addr, label)
  if not addr or addr == "" then error(label .. ": address not configured") end
  local full = component.get(addr)
  if not full then error(label .. ": component '" .. addr .. "' not found") end
  return component.proxy(full)
end

assert(nodeConf.dbAddr and nodeConf.dbAddr ~= "", "dbAddr not set in job_node_config.lua")
local dbAddr = component.get(nodeConf.dbAddr)
assert(dbAddr, "database component '" .. nodeConf.dbAddr .. "' not found")
local db = component.proxy(dbAddr)

local modules = {}
for i, mc in ipairs(nodeConf.modules) do
  local lbl = "Module " .. i
  modules[i] = {
    index      = i,
    tier       = mc.tier,
    conf       = mc,
    adapter    = getProxy(mc.moduleAddr,     lbl .. " moduleAddr"),
    iface      = getProxy(mc.ifaceAddr,      lbl .. " ifaceAddr"),
    transposer = getProxy(mc.transposerAddr, lbl .. " transposerAddr"),
    status     = "IDLE",  -- IDLE | LOADING | RUNNING | DONE | ERROR
    job        = nil,
    doneTime   = nil,
    loadHandle = nil,     -- scheduler task handle while LOADING
    loadResult = nil,     -- set by the load task: { ok=bool, err=?, stats=? }
  }
end

-- (Modules are disabled/cleared after the dashboard frame is drawn, so boot
--  shows progress instead of a blank console. See initModules() below.)

-- =============================================================================
-- BROKER STATE
-- =============================================================================

local brokerState = {
  dust = {}, plasma = {}, drones = {}, drills = {}, jobs = {}, cooldowns = {},
  lastDustSyncTime = 0, lastFluidSyncTime = 0, lastHWSyncTime = 0,
  lastDustSync = "--:--:--", lastFluidSync = "--:--:--", lastHWSync = "--:--:--",
  nextTarget = nil, telemetryReady = false,
  priorityMode = "threshold",  -- "threshold" (lowest fill first) | "rarity" (dust priority first)
}

local drillKeyOrder = {
  "steel","titanium","tungstensteel","naquadah",
  "naquadahAlloy","neutronium","cosmicNeutronium","infinity","transcendentMetal"
}

for _, cond in ipairs(config.conditions) do
  brokerState.dust[cond.itemName] = { stock=0, threshold=cond.amountToMaintain }
end
for _, name in ipairs(config.plasmaKeyOrder) do brokerState.plasma[name] = 0 end
for _, key in ipairs(config.droneKeyOrder) do brokerState.drones[key] = 0 end
for _, key in ipairs(drillKeyOrder) do brokerState.drills[key] = { kits=0, tips=0, rods=0 } end

-- UI layout (three panels).
local W, H = gpu and gpu.maxResolution() or 120, 50
if gpu then gpu.setResolution(W, H) end
local P1 = 1
local P2 = math.floor(W / 3) + 1
local P3 = math.floor(W * 2 / 3) + 1
local PW = P2 - 2

local DISPATCH_INTERVAL = 0.2
local lastDispatchCheck = 0
local ERROR_TIMEOUT = 10
-- hw_telem.lua reports kits as matched individual items, not 64-item packs.
local CONSUMABLES_PER_JOB = 3 * 64
local lastErrorTime = {}
-- Keep expensive machine state reads off the hot scheduler path.
local MACHINE_POLL_INTERVAL = 0.5
local STARTUP_GRACE = 30          -- if never active, report ERROR instead of looping
local IDLE_CONFIRM = 5           -- tolerate short gaps between mining cycles
local TELEMETRY_TIMEOUT = 40    -- no new dispatch with stale fluid/HW readings

-- =============================================================================
-- MODULE LIFECYCLE
-- =============================================================================

-- Recovery uses known-working slot calls. This path runs only on job cleanup,
-- not while loading, so correctness matters more than a few saved ticks.
local function returnItemsToME(mod)
  local size = mod.transposer.getInventorySize(mod.conf.inputBusSide)
  if not size then return false, "input bus not accessible" end
  for slot = 1, size do
    local amount = mod.transposer.getSlotStackSize(mod.conf.inputBusSide, slot) or 0
    if amount > 0 then
      local moved = mod.transposer.transferItem(
        mod.conf.inputBusSide, mod.conf.interfaceSide, amount, slot)
      if type(moved) ~= "number" or moved < amount then
        return false, string.format("bus slot %d moved %s/%d to ME",
          slot, tostring(moved), amount)
      end
    end
  end
  for slot = 1, size do
    if (mod.transposer.getSlotStackSize(mod.conf.inputBusSide, slot) or 0) > 0 then
      return false, "items remain in bus after recovery"
    end
  end
  return true
end

local function clearInterfaceSlots(mod)
  mod.iface.setInterfaceConfiguration(1)
  mod.iface.setInterfaceConfiguration(2)
  mod.iface.setInterfaceConfiguration(3)
end

local function getOptimalDistance(moduleTier, asteroid, droneKey)
  local m = config.optimizationMatrix
  if m and m[moduleTier] and m[moduleTier][asteroid] and m[moduleTier][asteroid][droneKey] then
    return math.min(200, m[moduleTier][asteroid][droneKey])
  end
  return 50
end

-- Spawn a cooperative load task for a module. The task runs concurrently with
-- every other module's load AND with the UI/telemetry loop.
local function beginLoad(mod)
  mod.loadResult = nil
  mod.lastError = nil
  taskErrors["load-M" .. mod.index] = nil
  mod.loadStart = computer.uptime()
  mod.loadHandle = sched.spawn(function()
    -- Do not wrap a yielding loader task in pcall: some OC Lua backends cannot
    -- yield across that boundary. The scheduler logs crashes and pollLoad()
    -- notices a finished task that did not return a result.
    local ok, result = loader.run(mod, mod.job, {
      config = config, logger = logger, db = db, dbAddr = dbAddr,
    })
    mod.loadResult = ok and {ok = true, stats = result}
                        or {ok = false, err = result}
  end, "load-M" .. mod.index)
end

local function pollLoad(mod)
  if mod.loadHandle and mod.loadHandle.done() and not mod.loadResult then
    mod.loadResult = {ok = false, err = taskErrors["load-M" .. mod.index]
      or "loader task stopped without a result"}
  end
  if not mod.loadResult then return end
  local r = mod.loadResult
  mod.loadHandle = nil
  mod.loadResult = nil
  if not r.ok then
    mod.lastError = tostring(r.err)
    mod.status = "ERROR"
    lastErrorTime[mod.index] = computer.uptime()
    logger:error("[LOAD] M" .. mod.index .. " failed: " .. tostring(r.err))
    pcall(mod.adapter.setWorkAllowed, false)
    return
  end

  local s = r.stats or {}
  local cp = s.confirmPolls or {}
  local elapsed = mod.loadStart and computer.uptime() - mod.loadStart or 0
  local dbPolls = math.max(cp.drone or 0, cp.tip or 0, cp.rod or 0)
  mod.lastLoad = string.format("loaded %.1fs  db:%d buf:%d", elapsed,
    dbPolls, s.arrivePolls or 0)
  local stages = s.stages or {}
  logger:info(string.format(
    "[LOAD] M%d %.2fs: cleanup=%.2f fingerprint=%.2f stocking=%.2f transfer=%.2f verify=%.2f (db:%d buf:%d)",
    mod.index, elapsed, stages.cleanup or 0, stages.fingerprints or 0,
    stages.stocking or 0, stages.transfer or 0, stages.verify or 0,
    dbPolls, s.arrivePolls or 0))

  local ok, err = pcall(function()
    mod.adapter.setParameters(mod.conf.distanceParam, 0, mod.job.distance)
    mod.adapter.setWorkAllowed(true)
  end)
  if not ok then
    mod.status = "ERROR"
    mod.lastError = "start: " .. tostring(err)
    lastErrorTime[mod.index] = computer.uptime()
    pcall(mod.adapter.setWorkAllowed, false)
    logger:error("[LOAD] M" .. mod.index .. " could not start: " .. tostring(err))
    return
  end
  mod.status = "RUNNING"
  mod.runningSince = computer.uptime()
  mod.nextMachinePoll = 0
  mod.seenActive = false
  mod.inactiveSince = nil
  mod.job.startTime = os.time()  -- display-only, not elapsed time
  mod.job.startUptime = mod.runningSince
end

local function stepRunning(mod)
  local now = computer.uptime()
  if now < (mod.nextMachinePoll or 0) then return end
  mod.nextMachinePoll = now + MACHINE_POLL_INTERVAL
  local ok, active = pcall(mod.adapter.isMachineActive)
  if not ok then
    logger:error("[MACHINE] M" .. mod.index .. " state unreadable: " .. tostring(active))
    return -- never treat an API failure as an inactive mining cycle
  end
  if active then
    mod.seenActive = true
    mod.inactiveSince = nil
  elseif not mod.seenActive then
    if now - (mod.runningSince or now) >= STARTUP_GRACE then
      mod.status = "ERROR"
      mod.lastError = "machine never started"
      lastErrorTime[mod.index] = now
      pcall(mod.adapter.setWorkAllowed, false)
      logger:error("[MACHINE] M" .. mod.index .. " never started within " .. STARTUP_GRACE .. "s")
    end
  else
    mod.inactiveSince = mod.inactiveSince or now
    if now - mod.inactiveSince >= IDLE_CONFIRM then
      local disabled = pcall(mod.adapter.setWorkAllowed, false)
      if not disabled then
        logger:error("[MACHINE] M" .. mod.index .. " disable call failed")
        return
      end
      mod.status = "DONE"
    end
  end
end

local function stepDone(mod)
  if not mod.doneTime then
    local ok, result, err = pcall(function()
      mod.adapter.setWorkAllowed(false)
      clearInterfaceSlots(mod)  -- disable restocking before returning leftovers
      return returnItemsToME(mod)
    end)
    if not ok or result ~= true then
      mod.status = "ERROR"
      mod.lastError = "cleanup: " .. tostring(ok and err or result)
      lastErrorTime[mod.index] = computer.uptime()
      logger:error("[CLEANUP] M" .. mod.index .. " could not recover bus: "
        .. tostring(ok and err or result))
      return
    end
    mod.doneTime = computer.uptime()
  elseif computer.uptime() - mod.doneTime >= 1 then
    if mod.job then brokerState.jobs[mod.job.jobId] = nil end
    mod.job = nil
    mod.status = "IDLE"
    mod.doneTime = nil
    mod.runningSince = nil
    mod.seenActive = nil
    mod.inactiveSince = nil
    lastDispatchCheck = computer.uptime() - DISPATCH_INTERVAL
  end
end

local function stepModules()
  for _, mod in ipairs(modules) do
    if     mod.status == "LOADING" then pollLoad(mod)
    elseif mod.status == "RUNNING" then stepRunning(mod)
    elseif mod.status == "DONE"    then stepDone(mod)
    end
  end
end

-- =============================================================================
-- DISPATCH
-- =============================================================================

-- Prune stale job records (defensive; a job stuck >300s is cleaned up so its
-- bookkeeping entry doesn't linger). The per-asteroid cap reads live module
-- status, not this table, so this is just housekeeping.
local function pruneStaleJobs()
  local now = computer.uptime()
  for jobId, job in pairs(brokerState.jobs) do
    if job.startUptime and now - job.startUptime > 300 then
      brokerState.jobs[jobId] = nil
    end
  end
end

local function findNeedsList()
  local needs = {}
  for _, cond in ipairs(config.conditions) do
    local stock = (brokerState.dust[cond.itemName] and brokerState.dust[cond.itemName].stock) or 0
    local ratio = stock / cond.amountToMaintain
    if ratio < 1.0 then
      local entry = config.dustTargets[cond.itemName]
      local ast = entry and entry.asteroid
      if ast and config.asteroids[ast] then
        needs[#needs+1] = { itemName=cond.itemName, asteroid=ast, ratio=ratio, priority=entry.priority or 99 }
      end
    end
  end
  if brokerState.priorityMode == "rarity" then
    -- Rarity first: lowest dustTargets.priority number wins; ties broken by fill.
    table.sort(needs, function(a, b)
      if a.priority ~= b.priority then return a.priority < b.priority end
      return a.ratio < b.ratio
    end)
  else
    -- Threshold: most-depleted (lowest stock/target ratio) first.
    table.sort(needs, function(a, b)
      if a.ratio == b.ratio then return a.itemName < b.itemName end
      return a.ratio < b.ratio
    end)
  end
  return needs
end

local function getIdleModules()
  local idle = {}
  local now = computer.uptime()
  for i, mod in ipairs(modules) do
    if mod.status == "IDLE" then
      idle[#idle + 1] = mod
    elseif mod.status == "ERROR" then
      if not lastErrorTime[i] then lastErrorTime[i] = now end
      if now - lastErrorTime[i] >= ERROR_TIMEOUT
          and not (mod.loadHandle and not mod.loadHandle.done()) then
        -- Never silently reuse a module with active machinery or leftover
        -- inventory. A failed recovery remains ERROR and is retried later.
        local ok, result, message = pcall(function()
          mod.adapter.setWorkAllowed(false)
          if mod.adapter.isMachineActive() then
            return false, "machine is still active"
          end
          clearInterfaceSlots(mod)
          return returnItemsToME(mod)
        end)
        lastErrorTime[i] = now
        if ok and result == true then
          if mod.job then brokerState.jobs[mod.job.jobId] = nil end
          mod.status = "IDLE"
          mod.lastError = nil
          mod.job = nil
          mod.doneTime = nil
          mod.loadResult = nil
          mod.loadHandle = nil
          mod.runningSince = nil
          mod.seenActive = nil
          mod.inactiveSince = nil
          lastErrorTime[i] = nil
          logger:info("[RECOVERY] M" .. i .. " verified idle and empty; recovered")
          idle[#idle + 1] = mod
        else
          logger:error("[RECOVERY] M" .. i .. " deferred: "
            .. tostring(ok and message or result))
        end
      end
    end
  end
  return idle
end

local function tryDispatch(mod, asteroid, droneKey)
  local asteroidData = config.asteroids[asteroid]
  if not asteroidData then return false end
  if not droneKey or (brokerState.drones[droneKey] or 0) <= 0 then return false end

  local droneTier = config.droneTierKeys[droneKey]
  if droneTier < asteroidData.minDrone or droneTier > asteroidData.maxDrone then return false end

  local drillKey = config.droneDrillMap[droneTier]
  if not drillKey then return false end

  -- hw_telem.lua calls min(tips, rods) "kits": this is a count of ITEMS.
  -- Each three-stack job needs 192 of both before any module is committed.
  local drill = brokerState.drills[drillKey]
  if not drill or (drill.kits or 0) < CONSUMABLES_PER_JOB then return false end

  local jobId = nodeId .. "-" .. os.time() .. "-M" .. mod.index
  mod.status = "LOADING"
  mod.job = {
    jobId = jobId, asteroid = asteroid, droneKey = droneKey, drillKey = drillKey,
    distance = getOptimalDistance(mod.tier, asteroid, droneKey),
    parallels = config.moduleTiers[mod.tier].maxParallels,
    startTime = os.time(),
    -- These reservations cover allocations NOT YET reflected in the most
    -- recent HW_UPDATE. Once AE2 stock is observed to decrease, the counts
    -- below are reconciled and do not get subtracted a second time.
    pendingDrone = 1,
    pendingKits = CONSUMABLES_PER_JOB,
  }
  brokerState.jobs[jobId] = {
    moduleIndex = mod.index, asteroid = asteroid,
    startTime = os.time(), startUptime = computer.uptime()
  }
  brokerState.nextTarget = { asteroid = asteroid, reason = "dispatched" }

  beginLoad(mod)  -- non-blocking: spawns the cooperative load task
  return true
end

-- Avoid double-reserving materials that have already left AE2.
-- HW telemetry is a snapshot of CURRENT network stock. The broker reserves
-- only allocations that are not yet visible as a stock decrease in a snapshot.
-- For example: AE2 reports 2 MK-IX drones, we dispatch 1 (pending=1), then
-- AE2 reports 1. The pending reservation is acknowledged (pending=0), so the
-- remaining 1 drone stays available to another module.
--
-- The hardware relay currently omits entries with stock=0. Missing entries
-- must therefore NOT be interpreted as an observed decrease by this broker.
-- Such pending reservations are conservatively retained until they can be
-- reconciled or the module safely returns its items and drops its job.
local function reconcileHardwareReservations(newDrones, newDrills)
  -- Compare successive positive-count reports for each drone tier.
  for _, key in ipairs(config.droneKeyOrder) do
    local newCount = newDrones and newDrones[key]
    if type(newCount) == "number" and newCount >= 0 then
      local previous = brokerState.drones[key] or 0
      local decrease = math.max(0, previous - newCount)
      if decrease > 0 then
        for _, mod in ipairs(modules) do
          local job = mod.job
          if job and job.droneKey == key and (job.pendingDrone or 0) > 0 then
            local acknowledged = math.min(job.pendingDrone, decrease)
            job.pendingDrone = job.pendingDrone - acknowledged
            decrease = decrease - acknowledged
            if decrease <= 0 then break end
          end
        end
      end
    end
  end

  -- Drill 'kits' are a number of matched tip/rod ITEMS, not stacks.
  -- One job reserves 192 tips and 192 rods. Just like drones, only keep
  -- reservations for quantities not yet reflected in a subsequent snapshot.
  for _, key in ipairs(drillKeyOrder) do
    local reading = newDrills and newDrills[key]
    local newCount = type(reading) == "table" and reading.kits or nil
    if type(newCount) == "number" and newCount >= 0 then
      local previous = (brokerState.drills[key] or {}).kits or 0
      local decrease = math.max(0, previous - newCount)
      if decrease > 0 then
        for _, mod in ipairs(modules) do
          local job = mod.job
          if job and job.drillKey == key and (job.pendingKits or 0) > 0 then
            local acknowledged = math.min(job.pendingKits, decrease)
            job.pendingKits = job.pendingKits - acknowledged
            decrease = decrease - acknowledged
            if decrease <= 0 then break end
          end
        end
      end
    end
  end
end

-- Count only withdrawals that are still outstanding against the latest HW
-- snapshot. Busy miners whose items were already deducted from AE2 are NOT
-- subtracted a second time.
local function availableDrones()
  local avail = {}
  for key, count in pairs(brokerState.drones) do avail[key] = count end
  for _, mod in ipairs(modules) do
    local job = mod.job
    if job and (job.pendingDrone or 0) > 0 then
      local k = job.droneKey
      avail[k] = (avail[k] or 0) - job.pendingDrone
    end
  end
  return avail
end

local function availableKits()
  local avail = {}
  for key, d in pairs(brokerState.drills) do
    avail[key] = (d and d.kits) or 0
  end
  for _, mod in ipairs(modules) do
    local job = mod.job
    if job and (job.pendingKits or 0) > 0 then
      local k = job.drillKey
      avail[k] = (avail[k] or 0) - job.pendingKits
    end
  end
  return avail
end

-- Per-asteroid module cap: an asteroid may hold at most "half the modules plus
-- one" at once, so a single high-tier target (e.g. Infinity Catalyst) can take a
-- majority but never starve every other need. Scales with total module count, so
-- it stays correct if this broker grows back into a multi-job-node fleet (up to
-- 24 modules across multiple space elevators, like v1.0).
local function asteroidCap()
  return math.floor(#modules / 2) + 1   -- 6 modules -> 4, 24 -> 13
end

-- Count modules currently committed (loading/running) to each asteroid.
local function activeAsteroidCounts()
  local counts = {}
  for _, mod in ipairs(modules) do
    if mod.status ~= "IDLE" and mod.job and mod.job.asteroid then
      counts[mod.job.asteroid] = (counts[mod.job.asteroid] or 0) + 1
    end
  end
  return counts
end

-- Mining modules physically require a plasma fluid to operate (any of the five
-- supported plasmas works; higher tiers just improve results). If we have none,
-- a dispatched module would load fine but never actually mine — so don't dispatch.
local function hasPlasma()
  for _, name in ipairs(config.plasmaKeyOrder) do
    if (brokerState.plasma[name] or 0) > 0 then return true end
  end
  return false
end

local function dispatchBatch()
  pruneStaleJobs()
  if not hasPlasma() then return end

  local idleModules = getIdleModules()
  if #idleModules == 0 then return end
  local needs = findNeedsList() -- already sorted (threshold or rarity mode)
  if #needs == 0 then return end

  local avail = availableDrones()
  local availKit = availableKits()
  local cap = asteroidCap()
  local astCount = activeAsteroidCounts()
  local seenAsteroids = {}
  local moduleTierNumbers = { ["MK-I"] = 1, ["MK-II"] = 2, ["MK-III"] = 3 }

  -- PRIORITY FIRST, best compatible drone SECOND. Never use pairs() for
  -- dispatch order, because pairs() destroys the sorted needs priority.
  for _, need in ipairs(needs) do
    if #idleModules == 0 then break end
    local asteroidName = need.asteroid
    if not seenAsteroids[asteroidName] then
      seenAsteroids[asteroidName] = true
      local asteroidData = config.asteroids[asteroidName]
      if asteroidData then
        for _, droneKey in ipairs(config.droneKeyOrder) do
          if #idleModules == 0 or (astCount[asteroidName] or 0) >= cap then
            break
          end
          local droneTier = config.droneTierKeys[droneKey]
          local drillKey = config.droneDrillMap[droneTier]
          if droneTier >= asteroidData.minDrone
              and droneTier <= asteroidData.maxDrone
              and drillKey then
            while #idleModules > 0
                and (astCount[asteroidName] or 0) < cap
                and (avail[droneKey] or 0) > 0
                and (availKit[drillKey] or 0) >= CONSUMABLES_PER_JOB do
              local assigned = false
              for idx = #idleModules, 1, -1 do
                local mod = idleModules[idx]
                -- Respect the asteroid's module-tier requirement, too.
                if (moduleTierNumbers[mod.tier] or 0) >= (asteroidData.minModule or 1)
                    and tryDispatch(mod, asteroidName, droneKey) then
                  astCount[asteroidName] = (astCount[asteroidName] or 0) + 1
                  avail[droneKey] = avail[droneKey] - 1
                  availKit[drillKey] = availKit[drillKey] - CONSUMABLES_PER_JOB
                  table.remove(idleModules, idx)
                  assigned = true
                  break
                end
              end
              if not assigned then break end
            end
          end
        end
      end
    end
  end
end
-- =============================================================================
-- TELEMETRY
-- =============================================================================

-- MEDINA_BROKER_DUST_CONFIG: broker is the only owner of dust targets.
-- The dust relay requests and receives these over the wireless telemetry port.
local dustTargetEntries = {}
for _, cond in ipairs(config.conditions) do
  assert(type(cond.itemName) == "string" and cond.itemName ~= "",
    "Dust target name is missing")
  assert(type(cond.amountToMaintain) == "number" and cond.amountToMaintain > 0,
    "Invalid dust target quantity for " .. cond.itemName)
  dustTargetEntries[#dustTargetEntries + 1] = {
    name = cond.itemName, threshold = cond.amountToMaintain
  }
end

-- Keep in sync with the dust-node protocol: the broker generates the ID and
-- the dust node echoes it to prevent old/mismatched readings being accepted.
local function dustTargetsId(entries)
  local h = 5381
  for _, entry in ipairs(entries) do
    local value = entry.name .. ":" .. tostring(entry.threshold) .. ";"
    for i = 1, #value do
      h = (h * 33 + value:byte(i)) % 2147483647
    end
  end
  return tostring(h)
end
local activeDustConfigId = dustTargetsId(dustTargetEntries)

local function broadcastDustTargets()
  modem.broadcast(config.ports.telemetry, serial.serialize({
    protocol = "MEDINA_TELEMETRY",
    sender = "MEDINA-BrokerMK3",
    payloadType = "DUST_TARGETS",
    configId = activeDustConfigId,
    data = dustTargetEntries
  }))
end

local function processMessage(evType, _, _, port, _, rawMsg)
  if evType ~= "modem_message" or port ~= config.ports.telemetry
      or type(rawMsg) ~= "string" then return end
  local ok, msg = pcall(serial.unserialize, rawMsg)
  if not ok or type(msg) ~= "table" or msg.protocol ~= "MEDINA_TELEMETRY" then return end

  -- The dust node can reboot at any time without having a local config.lua.
  if msg.payloadType == "DUST_TARGETS_REQUEST" then
    broadcastDustTargets()
    return
  end
  if type(msg.data) ~= "table" then return end

  if msg.payloadType == "DUST_UPDATE" then
    -- A scan from a different configuration must not change broker decisions.
    if msg.configId ~= activeDustConfigId then return end

    -- Check ALL configured items before updating ANY stock values: a broken,
    -- partial, or failed scan must not be reported as zero or fresh telemetry.
    for _, cond in ipairs(config.conditions) do
      local entry = msg.data[cond.itemName]
      if type(entry) ~= "table" or type(entry.stock) ~= "number"
          or entry.stock < 0 or entry.stock ~= entry.stock
          or entry.stock == math.huge then
        return
      end
    end
    for _, cond in ipairs(config.conditions) do
      brokerState.dust[cond.itemName] = {
        stock = msg.data[cond.itemName].stock,
        threshold = cond.amountToMaintain
      }
    end
    brokerState.lastDustSyncUptime = computer.uptime()
    brokerState.lastDustSyncTime = os.time()
    brokerState.lastDustSync = os.date("%X")
  elseif msg.payloadType == "FLUID_UPDATE" and msg.data.plasmas then
    for name, amount in pairs(msg.data.plasmas) do
      if brokerState.plasma[name] ~= nil then brokerState.plasma[name] = amount end
    end
    brokerState.lastFluidSyncTime = os.time()
    brokerState.lastFluidSyncUptime = computer.uptime()
    brokerState.lastFluidSync = os.date("%X")
  elseif msg.payloadType == "HW_UPDATE" then
    -- Reconcile unobserved reservations BEFORE overwriting previous stock.
    -- The relay's positive-only snapshot convention is preserved here.
    reconcileHardwareReservations(msg.data.drones, msg.data.drills)
    if msg.data.drones then for k, v in pairs(msg.data.drones) do brokerState.drones[k] = v end end
    if msg.data.drills then for k, v in pairs(msg.data.drills) do brokerState.drills[k] = v end end
    brokerState.lastHWSyncTime = os.time()
    brokerState.lastHWSyncUptime = computer.uptime()
    brokerState.lastHWSync = os.date("%X")
  end
end

-- =============================================================================
-- UI
-- =============================================================================

local function getSyncColor(t)
  if not t or t == 0 then return 0x555555 end
  local ago = (os.time() - t) / 20
  if ago < 60 then return 0x00FF00 elseif ago < 120 then return 0xFFAA00 else return 0xFF4444 end
end

local function formatQty(n)
  if n >= 1000000 then return string.format("%.1fm", n / 1000000)
  elseif n >= 1000 then return string.format("%.0fk", n / 1000)
  else return tostring(n) end
end

-- Display lines are cached separately for each panel.  This eliminates
-- clear-then-rewrite flicker and prevents old lines persisting when rows move.
local panelCache = {modules = {}, dust = {}, hw = {}}
local lastSyncText = nil
local function invalidatePanelCache()
  lastSyncText = nil
  panelCache = {modules = {}, dust = {}, hw = {}}
end
local function paintPanel(x, lines, cached)
  local padding = string.rep(" ", PW)
  gpu.setBackground(0x000000)
  for y = 6, H do
    local entry = lines[y]
    local content = entry and entry.text:sub(1, PW) or ""
    local color = entry and entry.color or 0x666666
    local previous = cached[y]
    if not previous or previous.text ~= content or previous.color ~= color then
      gpu.setForeground(color)
      -- Write the complete line once. No intermediate empty frame, no wrap.
      gpu.set(x, y, (content .. padding):sub(1, PW))
      cached[y] = {text = content, color = color}
    end
  end
end
local function newWriter()
  local lines, row = {}, 6
  local function put(text, color)
    if row <= H then
      lines[row] = {text = text or "", color = color or 0xFFFFFF}
    end
    row = row + 1
  end
  local function skip(n) row = row + (n or 1) end
  return lines, put, skip
end

local function drawModulePanel()
  local lines, put = newWriter()
  for _, mod in ipairs(modules) do
    if mod.status == "RUNNING" then
      put(string.format("  M%d [%-5s]  %s", mod.index, mod.tier,
          mod.job and mod.job.asteroid or "?"), 0xFFAA00)
    elseif mod.status == "LOADING" then
      put(string.format("  M%d [%-5s]  LOADING %s", mod.index, mod.tier,
          mod.job and mod.job.asteroid or ""), 0xFFFF00)
    elseif mod.status == "ERROR" then
      put(string.format("  M%d [%-5s]  ERROR", mod.index, mod.tier), 0xFF4444)
      if mod.lastError then
        put("    " .. tostring(mod.lastError):sub(1, PW - 4), 0xFF8888)
      end
    else
      put(string.format("  M%d [%-5s]  IDLE", mod.index, mod.tier), 0x555555)
    end
    if mod.status == "RUNNING" and mod.job then
      local droneName = config.drones[mod.job.droneKey] or "?"
      local lvl = droneName:match("MK%-(.+)") or "?"
      put(string.format("    dist=%d  drone=MK-%s", mod.job.distance or 0, lvl), 0xCCCCCC)
      if mod.lastLoad then put("    " .. mod.lastLoad, 0x668866) end
      put("", 0x666666)
    end
  end
  paintPanel(P1 + 1, lines, panelCache.modules)
end

local function drawDustPanel()
  local lines, put = newWriter()
  local list = {}
  for _, cond in ipairs(config.conditions) do
    local name = cond.itemName
    local stock = (brokerState.dust[name] and brokerState.dust[name].stock) or 0
    local ratio = stock / cond.amountToMaintain
    list[#list + 1] = {
      name = name, stock = stock, threshold = cond.amountToMaintain, ratio = ratio
    }
  end
  -- Stable tie order avoids unnecessary movement for identical stock ratios.
  table.sort(list, function(a, b)
    if a.ratio == b.ratio then return a.name < b.name end
    return a.ratio < b.ratio
  end)
  for _, item in ipairs(list) do
    local pct = math.floor(item.ratio * 100)
    local color = item.ratio >= 1.0 and 0x446644
        or item.ratio < 0.25 and 0xFF4444
        or item.ratio < 0.75 and 0xFFAA00 or 0x00FFFF
    local mark = item.ratio < 1.0 and "!" or " "
    put(string.format("  %s %-27s %3d%%", mark, item.name, pct), color)
    put(string.format("      %s / %s", formatQty(item.stock),
        formatQty(item.threshold)), 0x666666)
  end
  paintPanel(P2 + 1, lines, panelCache.dust)
end

local function drawHWPanel()
  local lines, put, skip = newWriter()
  if brokerState.nextTarget then
    put("  NEXT: " .. brokerState.nextTarget.asteroid, 0xFFAA00)
  else
    put("  NEXT: (idle)", 0x666666)
  end
  put("  PRIORITY: " .. brokerState.priorityMode:upper()
    .. "   CAP: " .. asteroidCap() .. "/asteroid", 0x666666)
  put("  TELEMETRY SYNC:", 0x666666)
  put("  Dust:   " .. brokerState.lastDustSync,
    getSyncColor(brokerState.lastDustSyncTime))
  put("  Fluid:  " .. brokerState.lastFluidSync,
    getSyncColor(brokerState.lastFluidSyncTime))
  put("  HW:     " .. brokerState.lastHWSync,
    getSyncColor(brokerState.lastHWSyncTime))
  skip(1)
  put("  TASKS RUNNING: " .. sched.count(), 0x888888)
  skip(1)
  put("  PLASMA STOCK:", 0x888888)
  local anyPlasma = false
  for _, name in ipairs(config.plasmaKeyOrder) do
    local amt = brokerState.plasma[name] or 0
    local short = name:gsub(" Plasma", "")
    put(string.format("  %-16s %8d mB", short, amt),
      amt > 0 and 0xFF00FF or 0x555555)
    if amt > 0 then anyPlasma = true end
  end
  if not anyPlasma then
    if brokerState.lastFluidSyncTime == 0 then
      put("  [ waiting for fluid telemetry... ]", 0xFFAA00)
    else
      put("  [ NO PLASMA - MINING BLOCKED ]", 0xFF4444)
    end
  end
  skip(1)
  put("  DRONES IN STOCK:", 0x888888)
  local anyDrone = false
  for _, key in ipairs(config.droneKeyOrder) do
    local count = brokerState.drones[key] or 0
    if count > 0 then
      local droneName = config.drones[key] or ("Drone-" .. key)
      local lvl = droneName:match("MK%-(.+)") or "?"
      put(string.format("  %-18s  x%d", "MK-" .. lvl, count), 0x00FFFF)
      anyDrone = true
    end
  end
  if not anyDrone then put("  [ NO DRONES IN STOCK ]", 0xFF4444) end
  skip(1)
  put("  DRILL KITS IN STOCK:", 0x888888)
  local anyDrill = false
  for _, key in ipairs(drillKeyOrder) do
    local d = brokerState.drills[key]
    local kits = (d and d.kits) or 0
    if kits > 0 then
      local entry = config.drills[key]
      local name = (entry and entry.tip and
        entry.tip:gsub(" Drill Tip", "")) or key
      put(string.format("  %-18s  x%d", name, kits), 0x00AAFF)
      anyDrill = true
    end
  end
  if not anyDrill then put("  [ NO DRILL KITS IN STOCK ]", 0xFF4444) end
  paintPanel(P3 + 1, lines, panelCache.hw)
end
local function drawStaticFrame()
  if not gpu then return end
  term.clear()
  gpu.setForeground(0x00FF00)
  gpu.fill(1, 1, W, 1, "="); gpu.fill(1, 5, W, 1, "=")
  term.setCursor(2, 2); gpu.setForeground(0xFFFFFF); io.write("MEDINA BROKER MK3  (v1.5)")
  term.setCursor(P1 + 1, 4); io.write("MODULES")
  term.setCursor(P2 + 1, 4); io.write("DUST STOCK")
  term.setCursor(P3 + 1, 4); io.write("HARDWARE")
  term.setCursor(2, 3); gpu.setForeground(0x888888)
  io.write("F10: safe stop / return bus items     R: refresh screen")
  gpu.setForeground(0x555555)
  for y = 6, H do
    term.setCursor(P1, y); io.write("|")
    term.setCursor(P2, y); io.write("|")
  end
end

local function drawUI()
  if not gpu then return end
  local syncText = "SYNC: " .. os.date("%H:%M:%S", math.floor(getUnixTime()))
  if syncText ~= lastSyncText then
    gpu.setBackground(0x000000)
    gpu.setForeground(0x555555)
    gpu.set(W - 17, 2, (syncText .. string.rep(" ", 18)):sub(1, 18))
    lastSyncText = syncText
  end
  drawModulePanel(); drawDustPanel(); drawHWPanel()
end

-- Boot-time prompt: how should the broker prioritize what to mine?
-- Runs once at startup, before the dashboard takes over the screen.
local function promptChoice(label, opts, default)
  print(label)
  for i, o in ipairs(opts) do print(string.format("  [%d]  %s", i, o)) end
  io.write("  Choice [1-" .. #opts .. "] (default " .. default .. "): ")
  local n = tonumber(io.read())
  if not n or n < 1 or n > #opts then n = default or 1 end
  return n
end

local function runBootPrompt()
  if gpu then
    term.clear(); term.setCursor(1, 1)
    gpu.setForeground(0x00FF00)
  end
  print("================================================================================")
  print("  MEDINA BROKER MK3 - STARTUP CONFIGURATION")
  print("================================================================================")
  if gpu then gpu.setForeground(0xFFFFFF) end

  local pr = promptChoice("\nSelect priority mode:", {
    "Threshold ratio  - mine the item with the LOWEST stock/target ratio first",
    "Rarity first     - mine highest dust-priority ores first, then by ratio",
  }, 1)
  brokerState.priorityMode = (pr == 2) and "rarity" or "threshold"

  logger:info("[STARTUP] priority mode = " .. brokerState.priorityMode)
  if gpu then gpu.setForeground(0x00FF00) end
  print("\n  Priority: " .. brokerState.priorityMode:upper() .. ".  Starting broker...")
  if gpu then gpu.setForeground(0xFFFFFF) end
  os.sleep(1)
end

-- Disable and clear every module's interface. Shows live progress in the MODULES
-- panel so boot feels responsive instead of staring at a blank console while ~24
-- component calls run. Cheap work; this is purely about feedback.
local function initModules()
  logger:info("[STARTUP] Initializing " .. #modules .. " modules...")
  for i, mod in ipairs(modules) do
    if gpu then
      local row = 5 + i
      gpu.fill(P1 + 1, row, PW, 1, " ")
      term.setCursor(P1 + 1, row)
      gpu.setForeground(0xFFFF00)
      io.write(string.format("  M%d [%-5s]  clearing...", mod.index, mod.tier))
    end
    pcall(function()
      mod.adapter.setWorkAllowed(false)
      mod.iface.setInterfaceConfiguration(1)
      mod.iface.setInterfaceConfiguration(2)
      mod.iface.setInterfaceConfiguration(3)
    end)
  end
end

-- Safe-stop key (F10): stop mining, allow current loader tasks to settle,
-- then recover input-bus items to ME. No new jobs are dispatched after F10.
local function gracefulShutdown()
  logger:info("[SHUTDOWN] F10 requested; no further dispatch")
  if gpu then
    term.clear()
    gpu.setForeground(0xFFFFFF)
  end
  print("MEDINA SAFE STOP")
  print("Disabling all mining modules...")

  local disabled = {}
  local warnings = {}
  local function warn(s)
    warnings[#warnings + 1] = s
    logger:error("[SHUTDOWN] " .. s)
    print("WARNING: " .. s)
  end

  -- Never pull a drone from a machine that is still allowed to start a recipe.
  for _, mod in ipairs(modules) do
    local ok, err = pcall(mod.adapter.setWorkAllowed, false)
    disabled[mod.index] = ok and err ~= false
    if not disabled[mod.index] then
      warn("M" .. mod.index .. " disable failed: " .. tostring(err))
    end
  end

  -- The loader has no cancellation API. Finish its cooperative tasks without
  -- calling pollLoad() (which would otherwise re-enable mining on success).
  print("Waiting for active item loaders...")
  local loadDeadline = computer.uptime() + 90
  while sched.busy() and computer.uptime() < loadDeadline do
    sched.tick()
    os.sleep(0.05)
  end
  if sched.busy() then
    warn("Loader timeout: recovery skipped to avoid racing item transfers")
    print("Modules are disabled where possible; check remaining input buses.")
    return false
  end

  -- The module may finish an in-progress mining recipe even after work is
  -- disabled. Wait for it to become inactive before touching its input bus.
  local allReturned = true
  for _, mod in ipairs(modules) do
    local prefix = "M" .. mod.index
    if not disabled[mod.index] then
      allReturned = false
      warn(prefix .. " skipped (not confirmed disabled)")
    else
      print(prefix .. ": waiting for mining cycle to finish...")
      local idle = false
      local deadline = computer.uptime() + 180
      while computer.uptime() < deadline do
        local ok, active = pcall(mod.adapter.isMachineActive)
        if not ok then
          warn(prefix .. " active state unreadable: " .. tostring(active))
          break
        end
        if not active then idle = true; break end
        os.sleep(0.25)
      end
      if not idle then
        allReturned = false
        warn(prefix .. " still active or unknown; bus left untouched")
      else
        -- Stop the ME interface from restocking items during recovery.
        local clearOK, clearErr = pcall(clearInterfaceSlots, mod)
        if not clearOK then
          allReturned = false
          warn(prefix .. " ME stocking config could not clear: " .. tostring(clearErr))
        else
          -- Empty EVERY slot, not just the drone/tip/rod slots. Retry until
          -- the interface has room to accept the items or we time out.
          local returned = false
          local transferDeadline = computer.uptime() + 30
          while computer.uptime() < transferDeadline do
            local ok, result = pcall(function()
              local count = mod.transposer.getInventorySize(mod.conf.inputBusSide)
              if not count then error("input bus not accessible") end
              for slot = 1, count do
                local qty = mod.transposer.getSlotStackSize(
                  mod.conf.inputBusSide, slot) or 0
                if qty > 0 then
                  mod.transposer.transferItem(
                    mod.conf.inputBusSide, mod.conf.interfaceSide, qty, slot)
                end
              end
              for slot = 1, count do
                if (mod.transposer.getSlotStackSize(
                    mod.conf.inputBusSide, slot) or 0) > 0 then
                  return false
                end
              end
              return true
            end)
            if not ok then
              warn(prefix .. " bus recovery error: " .. tostring(result))
              break
            end
            if result then returned = true; break end
            os.sleep(0.5)
          end
          if not returned then
            allReturned = false
            warn(prefix .. " still has items in its input bus; check ME interface space")
          else
            -- The interface is a buffer; wait briefly for returned items to
            -- actually drain into ME before claiming successful recovery.
            local drained = false
            local drainDeadline = computer.uptime() + 20
            while computer.uptime() < drainDeadline do
              local ok, empty = pcall(function()
                local count = mod.transposer.getInventorySize(mod.conf.interfaceSide)
                if not count then error("interface buffer not accessible") end
                for slot = 1, count do
                  if (mod.transposer.getSlotStackSize(
                      mod.conf.interfaceSide, slot) or 0) > 0 then
                    return false
                  end
                end
                return true
              end)
              if not ok then
                warn(prefix .. " cannot verify ME interface drain: " .. tostring(empty))
                break
              end
              if empty then drained = true; break end
              os.sleep(0.5)
            end
            if not drained then
              allReturned = false
              warn(prefix .. " bus empty, but items remain/unverified in ME interface")
            else
              print(prefix .. ": input bus returned and ME interface drained")
            end
          end
        end
      end
    end
  end
  if allReturned then
    print("SAFE STOP COMPLETE: all accessible input buses recovered.")
    logger:info("[SHUTDOWN] all modules recovered")
  else
    print("SAFE STOP PARTIAL: inspect warnings and module inventories.")
    logger:error("[SHUTDOWN] some modules need manual recovery")
  end
  print("Broker stopped; returning to the OpenOS shell.")
  return allReturned
end

-- =============================================================================
-- MAIN LOOP
-- =============================================================================

runBootPrompt()     -- ask priority mode (runs while you're at the console)
logger:info("Waiting for telemetry...")
drawStaticFrame()   -- frame appears immediately
initModules()       -- then clear modules with visible progress

if modem.isOpen(config.ports.telemetry) then
  logger:info("Modem open on port " .. config.ports.telemetry)
else
  logger:error("Modem NOT open on port " .. config.ports.telemetry)
end

-- Each part of the loop runs at the cadence it actually needs, so the heavy GPU
-- redraw doesn't throttle the time-sensitive scheduler:
--   - scheduler + module lifecycle: every iteration (loads are time-sensitive)
--   - messages: serviced with a tiny event.pull timeout so we spin fast
--   - UI redraw: ~4x/second (humans don't need more; GPU calls are expensive)
--   - dispatch: every DISPATCH_INTERVAL
local UI_INTERVAL = 0.25          -- seconds between full UI repaints
local lastUIDraw  = 0

-- Broadcast on startup, periodically, and in response to dust-node requests.
-- Updating config.lua on the broker requires restarting this broker program.
broadcastDustTargets()
local nextDustTargetsBroadcast = computer.uptime() + 15

while true do
  if computer.uptime() >= nextDustTargetsBroadcast then
    broadcastDustTargets()
    nextDustTargetsBroadcast = computer.uptime() + 15
  end
  -- 1. Service one inbound message. Very short timeout: returns immediately if a
  --    message is waiting, otherwise yields the CPU for ~10ms and comes back so
  --    the scheduler keeps ticking fast.
  local ev = { event.pull(0.01) }
  if ev[1] == "key_down" and ev[4] == keyboard.keys.f10 then
    gracefulShutdown()
    break
  elseif ev[1] == "key_down" and ev[4] == keyboard.keys.r then
    invalidatePanelCache()
    drawStaticFrame()
  elseif ev[1] == "modem_message" then
    processMessage(table.unpack(ev))
  end

  -- 2. Advance every in-flight load task. This is the hot path — runs every
  --    iteration so concurrent loads progress as fast as the hardware allows.
  sched.tick()

  -- 3. Advance module lifecycle. Machine activity is checked at most every
  --    0.5s per RUNNING module, not once on every ~0.01s loop iteration.
  stepModules()

  -- 4. Telemetry-ready gate. All three telem sources are required: dust (what to
  --    mine), hardware (drones/kits available), and fluid (plasma — modules can't
  --    run without it). Wait for all three before dispatching.
  if not brokerState.telemetryReady then
    brokerState.telemetryReady = (brokerState.lastDustSyncTime > 0)
                             and (brokerState.lastHWSyncTime > 0)
                             and (brokerState.lastFluidSyncTime > 0)
  end

  -- 5. Dispatch on its own cadence.
  local now = computer.uptime()
  if brokerState.telemetryReady
      and brokerState.lastFluidSyncUptime
      and now - brokerState.lastFluidSyncUptime < TELEMETRY_TIMEOUT
      and brokerState.lastHWSyncUptime
      and now - brokerState.lastHWSyncUptime < TELEMETRY_TIMEOUT
      and brokerState.lastDustSyncUptime
      and now - brokerState.lastDustSyncUptime < 35
      and (now - lastDispatchCheck >= DISPATCH_INTERVAL) then
    dispatchBatch()
    lastDispatchCheck = now
  end

  -- 6. Redraw the UI a few times a second, not every iteration. The full
  --    three-panel repaint is the most expensive thing we do; throttling it
  --    frees the loop to tick the scheduler hundreds of times per second.
  local up = computer.uptime()
  if up - lastUIDraw >= UI_INTERVAL then
    drawUI()
    lastUIDraw = up
  end
end
