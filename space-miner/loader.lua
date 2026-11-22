-- =============================================================================
-- MEDINA LOADER (v1.5, GTNH 2.8.4 performance revision)
--
-- Uses transposer.getAllStacks(side).getAll() for batched inventory snapshots.
-- GTNH OpenComputers synchronizes individual world interactions to the server
-- thread, so scanning slots via repeated getStackInSlot/getSlotStackSize calls
-- can cost many ticks even when db:1 and buf:1.
--
-- Preserves the original three dedicated database slots per mining module, the
-- 1 drone + 64 tips + 64 rods format, and scheduler.lua's cooperative tasks.
-- =============================================================================
local component = require("component")
local computer  = require("computer")
local sched     = dofile("/home/scheduler.lua")

local loader = {}
local CONFIRM_TIMEOUT = 8
local ARRIVE_TIMEOUT  = 8
local POLL_INTERVAL   = 0.1
local TIPS_PER        = 64
local RODS_PER        = 64

local function dbSlotsFor(modIndex)
  local base = (modIndex - 1) * 3
  return base + 1, base + 2, base + 3
end

local function pollUntil(predicate, timeout)
  local polls = 0
  local met = sched.await(function()
    polls = polls + 1
    return predicate()
  end, timeout, POLL_INTERVAL)
  return met, polls
end

-- A single bulk read gives a slot-indexed array (including empty positions).
-- Do NOT use ipairs or # on the result: empty slots can create holes.
local function snapshot(transposer, side)
  local iterator, err = transposer.getAllStacks(side)
  if not iterator then error("getAllStacks failed: " .. tostring(err), 2) end
  local entries = iterator.getAll()
  if type(entries) ~= "table" then
    error("getAllStacks.getAll() did not return a table", 2)
  end
  return entries
end

local function hasAny(stacks)
  for _, stack in pairs(stacks) do
    if type(stack) == "table" and (stack.size or 0) > 0 then return true end
  end
  return false
end

local function findSlot(stacks, label, count)
  for slot, stack in pairs(stacks) do
    if type(slot) == "number" and type(stack) == "table"
        and stack.label == label and (stack.size or 0) >= count then
      return slot
    end
  end
  return nil
end

local function clearInterfaceSlots(mod)
  -- Also turns off any stocking left by a previous/failed job.
  for i = 1, 3 do mod.iface.setInterfaceConfiguration(i) end
end

local function clearInputBus(mod)
  local stacks = snapshot(mod.transposer, mod.conf.inputBusSide)
  for slot, stack in pairs(stacks) do
    if type(slot) == "number" and type(stack) == "table"
        and (stack.size or 0) > 0 then
      local moved = mod.transposer.transferItem(
        mod.conf.inputBusSide, mod.conf.interfaceSide, stack.size, slot)
      if type(moved) ~= "number" or moved < stack.size then
        return false, string.format(
          "bus cleanup: slot %d returned %s/%d items", slot,
          tostring(moved), stack.size)
      end
    end
  end
  return true
end

local function confirmFingerprint(db, slot, expectedLabel)
  return pollUntil(function()
    local stack = db.get(slot)
    return stack ~= nil and stack.label == expectedLabel
  end, CONFIRM_TIMEOUT)
end

function loader.run(mod, job, deps)
  local config = deps.config
  local db     = deps.db
  local dbAddr = deps.dbAddr
  local droneName  = config.drones[job.droneKey]
  local drillEntry = config.drills[job.drillKey]
  if not droneName then return false, "bad droneKey: " .. tostring(job.droneKey) end
  if not drillEntry then return false, "bad drillKey: " .. tostring(job.drillKey) end

  local dbDrone, dbTip, dbRod = dbSlotsFor(mod.index)
  local stats = { confirmPolls = {}, arrivePolls = 0, stages = {} }
  local t = computer.uptime()
  local function stage(name)
    local now = computer.uptime()
    stats.stages[name] = now - t
    t = now
  end
  local function fail(reason)
    -- Disable stocking on failures. Leave any transferred items in the bus;
    -- the broker must verify recovery before it may reuse the module.
    pcall(clearInterfaceSlots, mod)
    return false, reason
  end

  -- 1. Remove old stocking BEFORE returning any leftovers, preventing
  --    simultaneous restocking while the input bus is being cleared.
  clearInterfaceSlots(mod)
  local busOK, busErr = clearInputBus(mod)
  if not busOK then return fail(busErr) end
  local busEmpty = pollUntil(function()
    return not hasAny(snapshot(mod.transposer, mod.conf.inputBusSide))
  end, ARRIVE_TIMEOUT)
  if not busEmpty then return fail("input bus did not empty before loading") end

  db.clear(dbDrone); db.clear(dbTip); db.clear(dbRod)
  -- The interface's buffer must be empty to avoid cross-contamination.
  local drained = pollUntil(function()
    return not hasAny(snapshot(mod.transposer, mod.conf.interfaceSide))
  end, ARRIVE_TIMEOUT)
  if not drained then return fail("ME interface buffer did not drain") end
  stage("cleanup")

  -- 2. Write and confirm the three fingerprints, using dedicated slots.
  local items = {
    {slot = dbDrone, label = droneName, tag = "drone"},
    {slot = dbTip,   label = drillEntry.tip, tag = "tip"},
    {slot = dbRod,   label = drillEntry.rod, tag = "rod"},
  }
  for _, it in ipairs(items) do
    local stored = mod.iface.store({label = it.label}, dbAddr, it.slot)
    local confirmed, polls = confirmFingerprint(db, it.slot, it.label)
    stats.confirmPolls[it.tag] = polls
    if not confirmed then
      return fail("fingerprint not confirmed for " .. it.tag .. " (" .. it.label
        .. "), store() returned " .. tostring(stored))
    end
  end
  stage("fingerprints")

  -- 3. Configure one stack of each consumable; wait using a SINGLE bulk
  --    interface snapshot per check, not three separate nine-slot scans.
  mod.iface.setInterfaceConfiguration(1, dbAddr, dbDrone, 1)
  mod.iface.setInterfaceConfiguration(2, dbAddr, dbTip, TIPS_PER)
  mod.iface.setInterfaceConfiguration(3, dbAddr, dbRod, RODS_PER)

  local arrived, polls = pollUntil(function()
    local stacks = snapshot(mod.transposer, mod.conf.interfaceSide)
    return findSlot(stacks, droneName, 1) ~= nil
       and findSlot(stacks, drillEntry.tip, TIPS_PER) ~= nil
       and findSlot(stacks, drillEntry.rod, RODS_PER) ~= nil
  end, ARRIVE_TIMEOUT)
  stats.arrivePolls = polls
  if not arrived then return fail("drone/tip/rod did not arrive in ME interface") end
  stage("stocking")

  -- 4. Always find each item by its identity, never by assumed interface slot.
  --    Take a fresh bulk snapshot for each move (AE2 can reorder/replenish).
  local function moveByIdentity(label, amount, destination, tag)
    local stacks = snapshot(mod.transposer, mod.conf.interfaceSide)
    local src = findSlot(stacks, label, amount)
    if not src then return false, tag .. " not in interface buffer" end
    local moved = mod.transposer.transferItem(
      mod.conf.interfaceSide, mod.conf.inputBusSide,
      amount, src, destination)
    if type(moved) ~= "number" or moved < amount then
      return false, tag .. " short transfer: " .. tostring(moved) .. "/" .. amount
    end
    return true
  end
  local ok, err = moveByIdentity(droneName, 1, 1, "drone")
  if not ok then return fail(err) end
  ok, err = moveByIdentity(drillEntry.tip, TIPS_PER, 2, "tip")
  if not ok then return fail(err) end
  ok, err = moveByIdentity(drillEntry.rod, RODS_PER, 3, "rod")
  if not ok then return fail(err) end
  -- Stop stocking as soon as the required items are transferred.
  clearInterfaceSlots(mod)
  stage("transfer")

  -- 5. Verify all bus slots in ONE bulk read, without an arbitrary 0.2s sleep.
  local result = snapshot(mod.transposer, mod.conf.inputBusSide)
  local drone = result[1]; local tip = result[2]; local rod = result[3]
  if not drone or drone.label ~= droneName or (drone.size or 0) < 1 then
    return fail("bus slot 1: drone mismatch / missing")
  end
  if not tip or tip.label ~= drillEntry.tip or (tip.size or 0) < TIPS_PER then
    return fail("bus slot 2: tip mismatch / shortfall")
  end
  if not rod or rod.label ~= drillEntry.rod or (rod.size or 0) < RODS_PER then
    return fail("bus slot 3: rod mismatch / shortfall")
  end
  stage("verify")
  return true, stats
end

loader.dbSlotsFor = dbSlotsFor
return loader
