-- =============================================================================
-- MEDINA LOADER v1.5 - compatibility/reliability repair for GTNH 2.8.4
--
-- Restores the upstream-known working inventory access and item movement path:
--   getStackInSlot + getSlotStackSize, rather than mandatory getAllStacks.
-- Keeps per-stage timings for identifying the real loading bottleneck.
-- Uses the original 3 DB fingerprints/module with 1 drone, 192 tips and 192 rods.
-- Requires a mining input bus with 7+ slots (MV or higher); keeps the
-- known-working single-stack transposer calls and AE2 interface API.
-- =============================================================================

local computer = require("computer")
local sched = dofile("/home/scheduler.lua")
local loader = {}

local CONFIRM_TIMEOUT = 8
local ARRIVE_TIMEOUT = 8
local POLL_INTERVAL = 0.1
-- Total per job: three full stacks of each consumable (384 items).
-- Each AE2 ME Interface stocking slot remains limited to one 64-item stack;
-- the loader consumes/reloads that slot three times instead of requesting 192.
local STACK_SIZE = 64
local STACKS_PER_CONSUMABLE = 3
local BUS_SLOTS_REQUIRED = 1 + 2 * STACKS_PER_CONSUMABLE
local TIPS_PER = STACK_SIZE
local RODS_PER = STACK_SIZE

local function dbSlotsFor(modIndex)
  local base = (modIndex - 1) * 3
  return base + 1, base + 2, base + 3
end

local function pollUntil(predicate, timeout)
  local polls = 0
  -- Scheduler evaluates predicates in a pcall; if one raises, it eventually
  -- times out. Record an error so a failure can be diagnosed in the broker.
  local lastError
  local met = sched.await(function()
    polls = polls + 1
    local ok, result = pcall(predicate)
    if not ok then lastError = tostring(result); return false end
    return result
  end, timeout, POLL_INTERVAL)
  return met, polls, lastError
end

local function clearInterfaceSlots(mod)
  for i = 1, 3 do mod.iface.setInterfaceConfiguration(i) end
end

local function clearInputBus(mod)
  local size = mod.transposer.getInventorySize(mod.conf.inputBusSide)
  if not size then return false, "input bus not accessible" end
  for slot = 1, size do
    local amount = mod.transposer.getSlotStackSize(mod.conf.inputBusSide, slot) or 0
    if amount > 0 then
      local moved = mod.transposer.transferItem(
        mod.conf.inputBusSide, mod.conf.interfaceSide, amount, slot)
      -- Do not silently proceed if items could not be returned to AE2.
      if type(moved) ~= "number" or moved < amount then
        return false, string.format("bus cleanup slot %d: moved %s / %d",
          slot, tostring(moved), amount)
      end
    end
  end
  return true
end

local function findSlot(mod, side, itemLabel, minimum)
  local count = mod.transposer.getInventorySize(side)
  if not count then return nil, "ME interface inventory inaccessible" end
  for slot = 1, count do
    local stack = mod.transposer.getStackInSlot(side, slot)
    if stack and stack.label == itemLabel and (stack.size or 0) >= minimum then
      return slot
    end
  end
  return nil
end

local function confirmFingerprint(db, slot, label)
  return pollUntil(function()
    local item = db.get(slot)
    return item and item.label == label
  end, CONFIRM_TIMEOUT)
end

function loader.run(mod, job, deps)
  local config, db, dbAddr = deps.config, deps.db, deps.dbAddr
  local droneName = config.drones[job.droneKey]
  local drill = config.drills[job.drillKey]
  if not droneName then return false, "invalid droneKey: " .. tostring(job.droneKey) end
  if not drill then return false, "invalid drillKey: " .. tostring(job.drillKey) end

  local dSlot, tSlot, rSlot = dbSlotsFor(mod.index)
  local stats = {confirmPolls = {}, arrivePolls = 0, stages = {}}
  local start = computer.uptime()
  local stageName = "cleanup"
  local function stage(nextName)
    local now = computer.uptime()
    stats.stages[stageName] = now - start
    start, stageName = now, nextName
  end
  local function fail(reason)
    -- Keep module disabled; never start mining after an incomplete load.
    pcall(mod.adapter.setWorkAllowed, false)
    pcall(clearInterfaceSlots, mod)
    return false, stageName .. ": " .. tostring(reason)
  end

  -- Validate capacity before changing any inventory. The drone uses slot 1;
  -- tips use slots 2-4 and rods use slots 5-7. Never write past the bus.
  local busSize = mod.transposer.getInventorySize(mod.conf.inputBusSide)
  if not busSize or busSize < BUS_SLOTS_REQUIRED then
    return fail(string.format("input bus has %s slots; need >= %d for 3+3 stacks",
      tostring(busSize), BUS_SLOTS_REQUIRED))
  end

  -- Stage 1: restore leftover items and make sure the three stocking slots
  -- are clear. Original MEDINA checks slots 1..3, not unrelated buffer slots.
  clearInterfaceSlots(mod)
  local cleared, clearErr = clearInputBus(mod)
  if not cleared then return fail(clearErr) end
  db.clear(dSlot); db.clear(tSlot); db.clear(rSlot)
  local drained, _, drainErr = pollUntil(function()
    for slot = 1, 3 do
      if (mod.transposer.getSlotStackSize(mod.conf.interfaceSide, slot) or 0) > 0 then
        return false
      end
    end
    return true
  end, ARRIVE_TIMEOUT)
  if not drained then return fail("interface slots 1-3 not empty" ..
    (drainErr and " (" .. drainErr .. ")" or "")) end
  stage("fingerprints")

  -- Stage 2: store item fingerprints; never assume store() is immediate.
  for _, item in ipairs({
    {slot = dSlot, label = droneName, tag = "drone"},
    {slot = tSlot, label = drill.tip, tag = "tip"},
    {slot = rSlot, label = drill.rod, tag = "rod"}
  }) do
    local result = mod.iface.store({label = item.label}, dbAddr, item.slot)
    local found, polls, err = confirmFingerprint(db, item.slot, item.label)
    stats.confirmPolls[item.tag] = polls
    if not found then
      return fail(item.tag .. " fingerprint not confirmed; store=" .. tostring(result)
        .. (err and ", error=" .. err or ""))
    end
  end
  stage("stocking")

  -- Stage 3: configure only the three proven ME Interface stocking slots.
  -- Never attempt to stock 192 in a single AE2 item slot (64 max).
  mod.iface.setInterfaceConfiguration(1, dbAddr, dSlot, 1)
  mod.iface.setInterfaceConfiguration(2, dbAddr, tSlot, TIPS_PER)
  mod.iface.setInterfaceConfiguration(3, dbAddr, rSlot, RODS_PER)

  -- Use the restored loader's exact-label slot lookup, not getAllStacks().
  local function waitForStock(includeDrone, tag)
    local arrived, polls, err = pollUntil(function()
      return (not includeDrone or findSlot(mod, mod.conf.interfaceSide, droneName, 1) ~= nil)
         and findSlot(mod, mod.conf.interfaceSide, drill.tip, TIPS_PER) ~= nil
         and findSlot(mod, mod.conf.interfaceSide, drill.rod, RODS_PER) ~= nil
    end, ARRIVE_TIMEOUT)
    stats.arrivePolls = stats.arrivePolls + polls
    if not arrived then
      return false, tag .. " not stocked within " .. ARRIVE_TIMEOUT .. "s"
        .. (err and " (" .. err .. ")" or "")
    end
    return true
  end

  local arrived, arriveErr = waitForStock(true, "first batch")
  if not arrived then return fail(arriveErr) end
  stage("transfer")

  -- Use the proven slot-finding + transposer.transferItem path. The source
  -- location is re-identified on each move because AE2 may refill/reorder.
  local function move(label, amount, destSlot, tag)
    local source, err = findSlot(mod, mod.conf.interfaceSide, label, amount)
    if not source then
      return false, tag .. " missing in interface" .. (err and ": " .. err or "")
    end
    local moved = mod.transposer.transferItem(
      mod.conf.interfaceSide, mod.conf.inputBusSide, amount, source, destSlot)
    if type(moved) ~= "number" or moved < amount then
      return false, tag .. " moved " .. tostring(moved) .. "/" .. amount
    end
    return true
  end

  -- Keep the drone in the original first bus slot. Clearing its stocking
  -- entry prevents a second drone being requested while consumables refill.
  local ok, err = move(droneName, 1, 1, "drone")
  if not ok then return fail(err) end
  mod.iface.setInterfaceConfiguration(1)

  for round = 1, STACKS_PER_CONSUMABLE do
    if round > 1 then
      ok, err = waitForStock(false, "refill batch " .. round)
      if not ok then return fail(err) end
    end

    -- Input Bus: slot 1 drone; 2,3,4 tips; 5,6,7 rods.
    local tipDest = 1 + round
    local rodDest = 1 + STACKS_PER_CONSUMABLE + round
    ok, err = move(drill.tip, TIPS_PER, tipDest, "tips batch " .. round)
    if not ok then return fail(err) end
    ok, err = move(drill.rod, RODS_PER, rodDest, "rods batch " .. round)
    if not ok then return fail(err) end
  end

  clearInterfaceSlots(mod)
  sched.sleep(0.2) -- Keep the proven GTNH/AE2 settling delay.
  stage("verify")

  -- Verify all seven destination slots. Any failed load stays disabled and
  -- the broker's existing recovery will return residual items to AE2.
  local function verify(slot, expected, minimum)
    local actual = mod.transposer.getStackInSlot(mod.conf.inputBusSide, slot)
    if not actual or actual.label ~= expected or (actual.size or 0) < minimum then
      return false, string.format("bus slot %d expected %s x%d, got %s",
        slot, expected, minimum,
        actual and (tostring(actual.label) .. " x" .. tostring(actual.size)) or "empty")
    end
    return true
  end

  ok, err = verify(1, droneName, 1)
  if not ok then return fail(err) end
  for round = 1, STACKS_PER_CONSUMABLE do
    ok, err = verify(1 + round, drill.tip, TIPS_PER)
    if not ok then return fail(err) end
    ok, err = verify(1 + STACKS_PER_CONSUMABLE + round, drill.rod, RODS_PER)
    if not ok then return fail(err) end
  end
  stage("done")
  return true, stats
end

loader.dbSlotsFor = dbSlotsFor
return loader
