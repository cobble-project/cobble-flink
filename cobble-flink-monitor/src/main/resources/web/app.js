const state = {
  meta: null,
  snapshots: [],
  inspectMode: 'scan',
  lastScanData: null,
  lastScanContext: null,
  previousPageCursors: [],
  currentPageCursor: null,
  nextPageCursor: null,
  pageNumber: 1,
  scanHasResult: false,
  scanLastUpdate: null,
  lookupLastUpdate: null,
  trackedLookups: [],
  latestRefreshTimer: null,
  latestRefreshInFlight: false,
  inspectRefreshTimer: null,
  inspectRefreshInFlight: false,
  inspectAutoRefreshEnabled: false,
  inspectAutoRefreshSeconds: 5,
  errorSource: null,
  listDisplayLimits: {},
}

const MAX_VALUE_DISPLAY_LENGTH = 300
const LIST_VALUE_PAGE_SIZE = 100

const $ = (id) => document.getElementById(id)

async function request(path, options = {}) {
  const response = await fetch(path, options)
  const data = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(data.error || `HTTP ${response.status}`)
  }
  return data
}

function showError(error, source = 'global') {
  const alert = $('alert')
  alert.textContent = error ? error.message || String(error) : ''
  alert.classList.toggle('hidden', !error)
  state.errorSource = error ? source : null
}

function clearError(source = 'global') {
  if (!state.errorSource || state.errorSource === source) {
    showError(null)
  }
}

function setLoading(loading) {
  $('refresh-button').disabled = loading
  $('open-new-path-button').disabled = loading
  $('new-source-path').disabled = loading
  $('operator-select').disabled = loading
  $('inspect-button').disabled = loading
  $('lookup-button').disabled = loading
  $('clear-lookup-button').disabled = loading || state.trackedLookups.length === 0
  $('state-key').disabled = loading
  $('namespace').disabled = loading
  $('map-key').disabled = loading
  $('prev-page-button').disabled = loading || state.previousPageCursors.length === 0
  $('next-page-button').disabled = loading || !state.nextPageCursor
}

async function refresh() {
  setLoading(true)
  try {
    const [meta, snapshots] = await Promise.all([
      request('/api/v1/meta'),
      request('/api/v1/snapshots'),
    ])
    state.meta = meta
    state.snapshots = snapshots.snapshots || []
    renderMeta()
    renderSnapshots()
    scheduleAutoRefresh()
    clearError('refresh')
  } catch (error) {
    showError(error, 'refresh')
  } finally {
    setLoading(false)
  }
}

function renderMeta() {
  const sourceOpen = Boolean(state.meta?.source_open)
  renderBucketRange()
  renderInspectTabs()
  renderLastUpdates()
  $('refresh-controls').classList.toggle('hidden', !sourceOpen)
  $('inspect-controls').classList.toggle('hidden', !sourceOpen)
  $('scan-controls').classList.toggle('hidden', !sourceOpen || state.inspectMode !== 'scan')
  $('lookup-controls').classList.toggle('hidden', !sourceOpen || state.inspectMode !== 'lookup')
  document.querySelector('#inspect-view .table-shell').classList.toggle('hidden', !sourceOpen)
  renderOperatorOptions()
  renderInspectTargets()
  renderStateFilterControls()
  renderActiveResult()
  renderPager()
}

function selectedCheckpointEntry() {
  const selectedId = Number(state.meta?.selected_checkpoint_id)
  return state.snapshots.find((checkpoint) => Number(checkpoint.id) === selectedId) || state.snapshots[0]
}

function renderOperatorOptions() {
  const checkpointMode = state.meta?.source_kind === 'checkpoint'
  $('operator-control').classList.toggle('hidden', !state.meta?.source_open || !checkpointMode)
  if (!checkpointMode) {
    $('operator-select').innerHTML = ''
    return
  }
  const checkpoint = selectedCheckpointEntry()
  const selectedOperator = state.meta?.selected_operator_id || ''
  const operatorSelect = $('operator-select')
  operatorSelect.innerHTML = ''
  for (const operator of checkpoint?.operators || []) {
    const option = document.createElement('option')
    option.value = operator.operator_id
    option.textContent = operator.operator_id
    operatorSelect.appendChild(option)
  }
  if ((checkpoint?.operators || []).some((operator) => operator.operator_id === selectedOperator)) {
    operatorSelect.value = selectedOperator
  }
}

function renderBucketRange() {
  const totalBuckets = Number(state.meta?.total_buckets || 0)
  if (totalBuckets > 0) {
    $('bucket-label').textContent = `Bucket (all or 0-${totalBuckets - 1})`
    $('bucket').max = String(totalBuckets - 1)
  } else {
    $('bucket-label').textContent = 'Bucket (all)'
    $('bucket').removeAttribute('max')
  }
  $('bucket').placeholder = totalBuckets > 0 ? `all or 0-${totalBuckets - 1}` : 'all'
}

function renderInspectTargets() {
  const select = $('inspect-target')
  const selected = select.value
  const targets = state.meta?.inspect_targets || []
  select.innerHTML = ''
  for (const target of targets) {
    const option = document.createElement('option')
    option.value = target.id
    option.textContent = target.kind === 'sink' ? 'sink' : `${target.name} (${target.kind})`
    select.appendChild(option)
  }
  if (targets.some((target) => target.id === selected)) {
    select.value = selected
  } else if (targets[0]) {
    select.value = targets[0].id
  }
  const active = activeTarget()
  $('target-control').classList.toggle(
    'hidden',
    targets.length === 0 || (targets.length <= 1 && active?.kind === 'sink'),
  )
  $('columns-control').classList.toggle('hidden', !active?.allows_columns)
  renderStateFilterControls()
}

function renderStateFilterControls() {
  const target = activeTarget()
  const schemaState = isSchemaStateTarget(target)
  const mapState = schemaState && target.state_kind === 'MAP'
  const voidNamespace = schemaState && isVoidNamespaceTarget(target)
  $('state-key-control').classList.toggle('hidden', !schemaState)
  $('namespace-control').classList.toggle('hidden', !schemaState || voidNamespace)
  $('map-key-control').classList.toggle('hidden', !mapState)
  $('prefix-control').classList.toggle('hidden', schemaState)
  if (schemaState) {
    $('state-key-label').textContent = `State key (${serializerLabel(target.serializer_classes?.key)})`
    $('state-key').removeAttribute('placeholder')
    $('namespace-label').textContent = `Namespace (${serializerLabel(target.serializer_classes?.namespace)})`
    $('namespace').placeholder = serializerPlaceholder(target.serializer_classes?.namespace)
    $('map-key-label').textContent = `Map key prefix (${serializerLabel(target.serializer_classes?.map_key)})`
    $('map-key').removeAttribute('placeholder')
  }
}

function renderSnapshots() {
  const sourceKind = state.meta?.source_kind || 'checkpoint'
  const checkpointMode = sourceKind === 'checkpoint'
  $('datasource-head').innerHTML = checkpointMode
    ? '<tr><th>Checkpoint</th><th>Path</th><th></th></tr>'
    : '<tr><th>Snapshot</th><th>Path</th><th></th></tr>'
  const body = $('snapshots-body')
  body.innerHTML = ''
  if (state.snapshots.length === 0) {
    body.innerHTML = '<tr><td colspan="3">Open a checkpoint root or Cobble data source.</td></tr>'
    return
  }
  renderLatestRows(body, checkpointMode, sourceKind)
  for (const snapshot of state.snapshots) {
    const selected = state.meta?.selected_checkpoint !== 'latest'
      && Number(state.meta?.selected_checkpoint_id) === Number(snapshot.id)
    const row = document.createElement('tr')
    const label = snapshotLabel(snapshot.id, sourceKind)
    row.innerHTML = `
      <td><code>${escapeHtml(label)}</code>${selected ? '<span class="check-mark">✓</span>' : ''}</td>
      <td><code>${escapeHtml(snapshot.directory)}</code></td>
      <td><button data-checkpoint-id="${snapshot.id}">Open</button></td>
    `
    body.appendChild(row)
  }
  body.querySelectorAll('button[data-checkpoint-id]').forEach((button) => {
    button.addEventListener('click', () => switchSource(button.dataset.checkpointId))
  })
}

function renderLatestRows(body, checkpointMode, sourceKind) {
  const latest = state.snapshots[0]
  if (!latest) return
  const selected = state.meta?.selected_checkpoint === 'latest'
  const row = document.createElement('tr')
  row.className = 'latest-row'
  const label = `latest (${snapshotLabel(latest.id, sourceKind)})`
  row.innerHTML = `
    <td><code>${escapeHtml(label)}</code>${selected ? '<span class="check-mark">✓</span>' : ''}</td>
    <td><code>${escapeHtml(latest.directory)}</code></td>
    <td><button data-checkpoint-id="latest">Open</button></td>
  `
  body.appendChild(row)
}

function snapshotLabel(id, sourceKind = state.meta?.source_kind || 'checkpoint') {
  return sourceKind === 'data_source' ? `snapshot-${id}` : `chk-${id}`
}

async function openNewPath() {
  const sourcePath = $('new-source-path').value.trim()
  if (!sourcePath) {
    showError(new Error('Enter a checkpoint root or Cobble data source path.'), 'source')
    return
  }
  setLoading(true)
  try {
    await switchSource('latest', '', sourcePath)
  } catch (error) {
    showError(error, 'source')
  } finally {
    setLoading(false)
  }
}

async function switchSource(
  checkpointId = 'latest',
  operatorId = '',
  source = '',
) {
  setLoading(true)
  try {
    const body = { checkpoint: checkpointId }
    if (operatorId) body.operator_id = operatorId
    if (source) body.source = source
    await request('/api/v1/mode', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    resetScanState()
    clearTrackedLookups()
    await refresh()
    clearError('source')
  } catch (error) {
    showError(error, 'source')
  } finally {
    setLoading(false)
  }
}

async function switchOperator() {
  const operatorId = $('operator-select').value
  if (!operatorId) return
  const checkpoint = state.meta?.selected_checkpoint === 'latest'
    ? 'latest'
    : String(state.meta?.selected_checkpoint_id || 'latest')
  await switchSource(checkpoint, operatorId)
}

function activeTarget() {
  const selected = $('inspect-target').value
  return (state.meta?.inspect_targets || []).find((target) => target.id === selected)
    || (state.meta?.inspect_targets || [])[0]
}

function activeTargetId() {
  return activeTarget()?.id || ''
}

function activeColumns() {
  return activeTarget()?.allows_columns ? $('columns').value.trim() : ''
}

function activeScanContext() {
  const target = activeTarget()
  return {
    targetId: target?.id || '',
    targetLabel: !target ? '' : target.kind === 'sink' ? 'sink' : `${target.name} (${target.kind})`,
    columns: activeColumns(),
    allowsColumns: Boolean(target?.allows_columns),
    targetKind: target?.kind || null,
    stateKind: target?.state_kind || null,
    serializerClasses: target?.serializer_classes || {},
  }
}

async function runInspect({ next = false } = {}) {
  if (state.inspectMode === 'lookup') {
    await runLookup()
  } else {
    await runScan(next ? 'next' : 'reset')
  }
}

async function runScan(direction = 'reset') {
  if (direction === 'next' && !state.nextPageCursor) return
  if (direction === 'previous' && state.previousPageCursors.length === 0) return
  const previousPagination = snapshotPagination()
  applyPageMove(direction)
  setLoading(true)
  try {
    const query = new URLSearchParams()
    query.set('mode', 'scan')
    const bucket = $('bucket').value.trim()
    if (bucket && bucket.toLowerCase() !== 'all') query.set('bucket', bucket)
    const target = activeTarget()
    if (isSchemaStateTarget(target)) {
      const stateKey = $('state-key').value.trim()
      const namespace = $('namespace').value.trim()
      const mapKey = $('map-key').value.trim()
      if (stateKey) query.set('state_key', stateKey)
      if (namespace) query.set('namespace', namespace)
      if (mapKey) query.set('map_key', mapKey)
    } else {
      query.set('prefix', $('prefix').value || '')
    }
    query.set('limit', $('limit').value || '50')
    if (activeTargetId()) query.set('target', activeTargetId())
    if (activeColumns()) query.set('columns', activeColumns())
    const scanContext = activeScanContext()
    if (state.currentPageCursor?.bucket != null) {
      query.set('start_bucket', String(state.currentPageCursor.bucket))
    }
    if (state.currentPageCursor?.keyB64) {
      query.set('start_after_b64', state.currentPageCursor.keyB64)
    }
    const data = await request(`/api/v1/inspect?${query}`)
    state.nextPageCursor = nextCursorFromScan(data.scan)
    state.scanHasResult = true
    state.lastScanData = data
    state.lastScanContext = scanContext
    state.scanLastUpdate = new Date()
    renderScanResult(data, scanContext)
    renderPager()
    renderLastUpdates()
    clearError('inspect')
  } catch (error) {
    restorePagination(previousPagination)
    renderPager()
    showError(error, 'inspect')
  } finally {
    setLoading(false)
  }
}

async function runLookup() {
  if (state.trackedLookups.length === 0) {
    renderLookupResult()
    return
  }
  setLoading(true)
  try {
    const groups = groupedTrackedLookups()
    for (const group of groups) {
      const query = new URLSearchParams()
      query.set('mode', 'lookup')
      query.set('target', group.targetId)
      if (group.columns) query.set('columns', group.columns)
      query.set(
        'lookup_items',
        JSON.stringify(group.items.map((item) => ({ bucket: item.bucket, key_b64: item.keyB64 }))),
      )
      const data = await request(`/api/v1/inspect?${query}`)
      const lookupItems = data.lookup || []
      lookupItems.forEach((result, index) => {
        const tracked = group.items[index]
        tracked.keyUtf8 = result.key_utf8 ?? tracked.keyUtf8
        tracked.value = result.value
        tracked.decodedKey = result.decoded_key || null
        tracked.decodedValue = result.decoded_value ?? null
        tracked.decodeError = result.decode_error || null
      })
    }
    state.lookupLastUpdate = new Date()
    renderLookupResult()
    renderPager()
    renderLastUpdates()
    clearError('inspect')
  } catch (error) {
    showError(error, 'inspect')
  } finally {
    setLoading(false)
  }
}

function scheduleAutoRefresh() {
  if (state.latestRefreshTimer) {
    clearInterval(state.latestRefreshTimer)
    state.latestRefreshTimer = null
  }
  if (!state.meta?.source_open || state.meta?.selected_checkpoint !== 'latest') {
    scheduleInspectAutoRefresh()
    return
  }
  const delay = Number(state.meta?.latest_refresh_millis || 5000)
  state.latestRefreshTimer = setInterval(async () => {
    if (state.latestRefreshInFlight) return
    state.latestRefreshInFlight = true
    try {
      await refresh()
    } finally {
      state.latestRefreshInFlight = false
    }
  }, delay)
  scheduleInspectAutoRefresh()
}

function scheduleInspectAutoRefresh() {
  if (state.inspectRefreshTimer) {
    clearInterval(state.inspectRefreshTimer)
    state.inspectRefreshTimer = null
  }
  if (!state.meta?.source_open || !state.inspectAutoRefreshEnabled) {
    return
  }
  const delay = Math.max(1, Number(state.inspectAutoRefreshSeconds || 5)) * 1000
  state.inspectRefreshTimer = setInterval(async () => {
    if (state.inspectRefreshInFlight) return
    if (state.inspectMode === 'scan' && !state.scanHasResult) return
    if (state.inspectMode === 'lookup' && state.trackedLookups.length === 0) return
    state.inspectRefreshInFlight = true
    try {
      if (state.inspectMode === 'lookup') {
        await runLookup()
      } else {
        await runScan('refresh')
      }
    } finally {
      state.inspectRefreshInFlight = false
    }
  }, delay)
}

function refreshTargetControls() {
  renderInspectTargets()
  resetScanState()
  setLoading(false)
}

function trackScanItem(trackId) {
  const context = state.lastScanContext
  const item = scanItems().find((candidate) => (
    trackIdentity(candidate.bucket, candidate.key_b64, context?.targetId, context?.columns) === trackId
  ))
  if (!item || !context) return
  if (state.trackedLookups.some((tracked) => tracked.id === trackId)) {
    switchInspectMode('lookup')
    return
  }
  state.trackedLookups.push({
    id: trackId,
    bucket: item.bucket,
    keyB64: item.key_b64,
    keyUtf8: item.key_utf8,
    targetId: context.targetId,
    targetLabel: context.targetLabel,
    columns: context.columns,
    allowsColumns: context.allowsColumns,
    targetKind: context.targetKind,
    stateKind: context.stateKind,
    serializerClasses: context.serializerClasses,
    value: item.columns || item.value || null,
    decodedKey: item.decoded_key || null,
    decodedValue: item.decoded_value ?? null,
    decodeError: item.decode_error || null,
  })
  switchInspectMode('lookup')
  renderLookupResult()
}

function trackIdentity(bucket, keyB64, targetId, columns) {
  return `${targetId || ''}|${columns || ''}|${bucket}|${keyB64}`
}

function scanItems() {
  return state.lastScanData?.scan?.items || []
}

function groupedTrackedLookups() {
  const groupsById = new Map()
  for (const item of state.trackedLookups) {
    const groupId = `${item.targetId}|${item.columns || ''}`
    if (!groupsById.has(groupId)) {
      groupsById.set(groupId, {
        targetId: item.targetId,
        columns: item.columns,
        items: [],
      })
    }
    groupsById.get(groupId).items.push(item)
  }
  return [...groupsById.values()]
}

function removeTrackedLookup(trackId) {
  state.trackedLookups = state.trackedLookups.filter((item) => item.id !== trackId)
  if (state.trackedLookups.length === 0) {
    state.lookupLastUpdate = null
    renderLastUpdates()
  }
  renderLookupResult()
  scheduleInspectAutoRefresh()
}

function clearTrackedLookups() {
  state.trackedLookups = []
  state.lookupLastUpdate = null
  renderLastUpdates()
}

function renderTrackedValue(item) {
  if (item.value == null) return `<span class="pill">missing</span>${renderDecodeError(item.decodeError)}`
  if (item.allowsColumns) return renderColumns(item.value)
  return renderStateValue(
    item.value,
    item.decodedValue,
    item.decodeError,
    trackedTarget(item),
    `track:${item.id}:value`,
  )
}

function snapshotPagination() {
  return {
    previousPageCursors: [...state.previousPageCursors],
    currentPageCursor: state.currentPageCursor,
    nextPageCursor: state.nextPageCursor,
    pageNumber: state.pageNumber,
    scanHasResult: state.scanHasResult,
  }
}

function restorePagination(pagination) {
  state.previousPageCursors = pagination.previousPageCursors
  state.currentPageCursor = pagination.currentPageCursor
  state.nextPageCursor = pagination.nextPageCursor
  state.pageNumber = pagination.pageNumber
  state.scanHasResult = pagination.scanHasResult
}

function resetPagination() {
  state.previousPageCursors = []
  state.currentPageCursor = null
  state.nextPageCursor = null
  state.pageNumber = 1
  state.scanHasResult = false
}

function resetScanState() {
  resetPagination()
  state.lastScanData = null
  state.lastScanContext = null
  state.scanLastUpdate = null
  state.listDisplayLimits = {}
}

function applyPageMove(direction) {
  if (direction === 'reset') {
    resetPagination()
    return
  }
  if (direction === 'next') {
    state.previousPageCursors.push(state.currentPageCursor)
    state.currentPageCursor = state.nextPageCursor
    state.nextPageCursor = null
    state.pageNumber += 1
    return
  }
  if (direction === 'previous') {
    state.currentPageCursor = state.previousPageCursors.pop() || null
    state.nextPageCursor = null
    state.pageNumber = Math.max(1, state.pageNumber - 1)
  }
}

function nextCursorFromScan(scan) {
  if (!scan?.next_start_after_b64) return null
  return {
    bucket: scan.next_start_bucket,
    keyB64: scan.next_start_after_b64,
  }
}

function renderPager() {
  const visible = Boolean(state.meta?.source_open)
    && state.inspectMode === 'scan'
    && state.scanHasResult
  $('scan-pager').classList.toggle('hidden', !visible)
  $('page-indicator').textContent = `Page ${state.pageNumber}`
  $('prev-page-button').disabled = state.previousPageCursors.length === 0
  $('next-page-button').disabled = !state.nextPageCursor
}

function invalidatePagination() {
  resetScanState()
  if (state.inspectMode === 'scan') {
    renderActiveResult()
  }
  renderPager()
  setLoading(false)
}

function renderInspectTabs() {
  $('scan-tab').classList.toggle('active', state.inspectMode === 'scan')
  $('lookup-tab').classList.toggle('active', state.inspectMode === 'lookup')
}

function switchInspectMode(mode) {
  state.inspectMode = mode
  renderInspectTabs()
  const sourceOpen = Boolean(state.meta?.source_open)
  $('scan-controls').classList.toggle('hidden', !sourceOpen || state.inspectMode !== 'scan')
  $('lookup-controls').classList.toggle('hidden', !sourceOpen || state.inspectMode !== 'lookup')
  renderActiveResult()
  renderPager()
  scheduleInspectAutoRefresh()
  setLoading(false)
}

function renderActiveResult() {
  if (!state.meta?.source_open) return
  if (state.inspectMode === 'lookup') {
    renderLookupResult()
    return
  }
  if (state.lastScanData) {
    renderScanResult(state.lastScanData, state.lastScanContext)
    return
  }
  renderScanResult({ scan: { items: [] }, inspect_target: activeTarget() }, activeScanContext())
}

function renderLastUpdates() {
  const value = state.inspectMode === 'lookup' ? state.lookupLastUpdate : state.scanLastUpdate
  $('current-last-update').textContent = formatTimestamp(value)
}

function formatTimestamp(value) {
  if (!value) return '-'
  return value.toLocaleTimeString()
}

function updateInspectAutoRefresh() {
  state.inspectAutoRefreshEnabled = $('auto-refresh-enabled').checked
  state.inspectAutoRefreshSeconds = Math.max(1, Number($('auto-refresh-seconds').value || 5))
  $('auto-refresh-seconds').value = String(state.inspectAutoRefreshSeconds)
  scheduleInspectAutoRefresh()
}

function renderScanResult(data, context = activeScanContext()) {
  const items = data.scan?.items || []
  const target = data.inspect_target || activeTarget()
  const sink = target?.allows_columns
  const timer = isTimerTarget(target)
  $('result-head').innerHTML = timer
    ? '<tr><th>Bucket</th><th>Timer key</th><th>Timestamp</th><th></th></tr>'
    : sink
    ? '<tr><th>Bucket</th><th>Key</th><th>Columns</th><th></th></tr>'
    : '<tr><th>Bucket</th><th>Key</th><th>Value</th><th></th></tr>'
  const body = $('result-body')
  body.innerHTML = ''
  for (const item of items) {
    const row = document.createElement('tr')
    const trackId = trackIdentity(item.bucket, item.key_b64, context.targetId, context.columns)
    if (timer) {
      row.innerHTML = `
        <td>${item.bucket}</td>
        <td>${renderTimerKey(item.key_b64, item.key_utf8, item.decoded_key, target)}</td>
        <td>${renderTimerTimestamp(item.decoded_key, item.decode_error)}</td>
        <td>${renderActionMenu(scanRowActions(item, target, context, trackId))}</td>
      `
    } else {
      row.innerHTML = `
        <td>${item.bucket}</td>
        <td>${renderKey(item.key_b64, item.key_utf8, item.decoded_key, target)}</td>
        <td>${sink ? renderColumns(item.columns) : renderStateValue(item.value, item.decoded_value, item.decode_error, target, `scan:${trackId}:value`)}</td>
        <td>${renderActionMenu(scanRowActions(item, target, context, trackId))}</td>
      `
    }
    body.appendChild(row)
  }
  if (items.length === 0) {
    body.innerHTML = '<tr><td colspan="4">No rows.</td></tr>'
  }
  wireRowActionButtons(body)
  wireListMoreButtons(body)
}

function renderLookupResult() {
  const items = state.trackedLookups
  const timerOnly = items.length > 0 && items.every((item) => isTimerTarget(trackedTarget(item)))
  $('result-head').innerHTML = timerOnly
    ? '<tr><th>Target</th><th>Bucket</th><th>Timer key</th><th>Timestamp</th><th></th></tr>'
    : '<tr><th>Target</th><th>Bucket</th><th>Key</th><th>Value</th><th></th></tr>'
  const body = $('result-body')
  body.innerHTML = ''
  for (const item of items) {
    const row = document.createElement('tr')
    const target = trackedTarget(item)
    if (timerOnly) {
      row.innerHTML = `
        <td>${escapeHtml(item.targetLabel)}</td>
        <td>${item.bucket}</td>
        <td>${renderTimerKey(item.keyB64, item.keyUtf8, item.decodedKey, target)}</td>
        <td>${renderTimerTimestamp(item.decodedKey, item.decodeError)}</td>
        <td>${renderActionMenu(trackedRowActions(item, target))}</td>
      `
    } else {
      row.innerHTML = `
        <td>${escapeHtml(item.targetLabel)}</td>
        <td>${item.bucket}</td>
        <td>${renderKey(item.keyB64, item.keyUtf8, item.decodedKey, target)}</td>
        <td>${isTimerTarget(target) ? renderTimerTimestamp(item.decodedKey, item.decodeError) : renderTrackedValue(item)}</td>
        <td>${renderActionMenu(trackedRowActions(item, target))}</td>
      `
    }
    body.appendChild(row)
  }
  if (items.length === 0) {
    body.innerHTML = '<tr><td colspan="5">No tracked entries.</td></tr>'
  }
  wireRowActionButtons(body)
  wireListMoreButtons(body)
}

function scanRowActions(item, target, context, trackId) {
  return [
    { action: 'track', id: trackId, label: 'Track' },
    ...rawCopyActions(item.key_b64, context?.allowsColumns ? item.columns : item.value),
  ]
}

function trackedRowActions(item, target) {
  return [
    { action: 'remove-track', id: item.id, label: 'Remove' },
    ...rawCopyActions(item.keyB64, item.value),
  ]
}

function rawCopyActions(rawKeyB64, rawValue) {
  const actions = []
  if (rawKeyB64 != null) {
    actions.push({ action: 'copy-text', label: 'Copy raw key', value: rawKeyB64 })
  }
  if (rawValue !== undefined && rawValue !== null) {
    actions.push({ action: 'copy-text', label: 'Copy raw value', value: rawCopyValue(rawValue) })
  }
  return actions
}

function renderActionMenu(actions) {
  return `
    <button class="row-action-trigger" data-actions="${escapeHtml(JSON.stringify(actions))}" aria-label="Row actions">
      ...
    </button>
  `
}

function wireRowActionButtons(root) {
  root.querySelectorAll('.row-action-trigger').forEach((button) => {
    button.addEventListener('click', (event) => {
      event.stopPropagation()
      openRowActionPopover(button)
    })
  })
}

function openRowActionPopover(trigger) {
  closeRowActionPopover()
  const actions = JSON.parse(trigger.dataset.actions || '[]')
  const menu = document.createElement('div')
  menu.id = 'row-action-popover'
  menu.className = 'row-actions-menu'
  for (const item of actions) {
    const button = document.createElement('button')
    button.textContent = item.label
    button.addEventListener('click', async (event) => {
      event.stopPropagation()
      closeRowActionPopover()
      await handleRowAction(item)
    })
    menu.appendChild(button)
  }
  document.body.appendChild(menu)
  positionRowActionPopover(trigger, menu)
}

function positionRowActionPopover(trigger, menu) {
  const triggerRect = trigger.getBoundingClientRect()
  const menuRect = menu.getBoundingClientRect()
  const margin = 6
  let top = triggerRect.bottom + margin
  if (top + menuRect.height > window.innerHeight - margin) {
    top = triggerRect.top - menuRect.height - margin
  }
  top = Math.max(margin, top)
  let left = triggerRect.right - menuRect.width
  left = Math.max(margin, Math.min(left, window.innerWidth - menuRect.width - margin))
  menu.style.top = `${Math.round(top)}px`
  menu.style.left = `${Math.round(left)}px`
}

function closeRowActionPopover() {
  document.querySelector('#row-action-popover')?.remove()
}

async function handleRowAction(item) {
  const action = item.action
  const rowId = item.id
  if (action === 'track') {
    trackScanItem(rowId)
    return
  }
  if (action === 'remove-track') {
    removeTrackedLookup(rowId)
    return
  }
  if (action === 'copy-text') {
    await copyText(item.value)
  }
}

function wireListMoreButtons(root) {
  root.querySelectorAll('button[data-list-display-id]').forEach((button) => {
    button.addEventListener('click', (event) => {
      event.stopPropagation()
      const id = button.dataset.listDisplayId
      state.listDisplayLimits[id] = Number(button.dataset.nextLimit || LIST_VALUE_PAGE_SIZE)
      renderActiveResult()
    })
  })
}

async function copyText(text) {
  const value = text == null ? '' : String(text)
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value)
      return
    }
  } catch (error) {
    // Fall through to the textarea fallback.
  }
  const textarea = document.createElement('textarea')
  textarea.value = value
  textarea.setAttribute('readonly', '')
  textarea.style.position = 'fixed'
  textarea.style.left = '-9999px'
  document.body.appendChild(textarea)
  textarea.select()
  document.execCommand('copy')
  textarea.remove()
}

function rawCopyValue(value) {
  if (value == null) return ''
  if (Array.isArray(value)) {
    return JSON.stringify(value.map((item) => {
      if (!item) return null
      if (isRawBytesJson(item)) return item.b64 ?? ''
      return item
    }))
  }
  if (isRawBytesJson(value)) {
    return value.b64 ?? ''
  }
  if (typeof value === 'object') {
    return value.b64 ?? JSON.stringify(value)
  }
  return String(value)
}

function renderValue(value) {
  if (!value) return '<span class="pill">null</span>'
  return `${renderCode(value.b64)}${renderUtf8Pill(value.utf8)}`
}

function renderStateValue(value, decodedValue, decodeError, target = null, displayId = '') {
  if (isDecodedTarget(target) && target?.kind === 'timer') {
    return `<span class="muted-text">timer entry</span>${renderDecodeError(decodeError)}`
  }
  if (isDecodedTarget(target) && decodedValue !== undefined && decodedValue !== null) {
    return `${renderDecodedSection(decodedValue, target, displayId)}${renderDecodeError(decodeError)}`
  }
  return `${renderValue(value)}${renderDecodeError(decodeError)}`
}

function renderColumns(columns = []) {
  return columns.map((column, index) => {
    if (!column) return `<div><strong>${index}</strong>: null</div>`
    return `<div><strong>${index}</strong>: ${renderCode(column.b64)}${renderUtf8Pill(column.utf8)}</div>`
  }).join('')
}

function renderKey(keyB64, keyUtf8, decodedKey = null, target = null) {
  if (isDecodedTarget(target) && decodedKey && typeof decodedKey === 'object') {
    const decoded = renderDecodedKey(decodedKey, target)
    if (decoded) return decoded
  }
  return `${renderCode(keyB64)}${renderUtf8Pill(keyUtf8)}${renderDecodedKey(decodedKey, target)}`
}

function renderTimerKey(keyB64, keyUtf8, decodedKey = null, target = null) {
  if (decodedKey && typeof decodedKey === 'object' && Object.prototype.hasOwnProperty.call(decodedKey, 'key')) {
    return renderDecodedValue(decodedKey.key, target?.serializer_classes?.key)
  }
  return `${renderCode(keyB64)}${renderUtf8Pill(keyUtf8)}`
}

function renderTimerTimestamp(decodedKey = null, decodeError = null) {
  if (decodedKey && typeof decodedKey === 'object' && Object.prototype.hasOwnProperty.call(decodedKey, 'timestamp')) {
    return `${renderDecodedValue(decodedKey.timestamp, 'org.apache.flink.api.common.typeutils.base.LongSerializer')}${renderDecodeError(decodeError)}`
  }
  return `<span class="muted-text">unknown</span>${renderDecodeError(decodeError)}`
}

function renderDecodedKey(decodedKey, target = null) {
  if (!decodedKey || typeof decodedKey !== 'object') return ''
  const rows = []
  const typeByLabel = {
    timestamp: null,
    key: target?.serializer_classes?.key,
    namespace: isVoidSerializerClass(target?.serializer_classes?.namespace)
      ? null
      : target?.serializer_classes?.namespace,
    map_key: target?.serializer_classes?.map_key,
  }
  const displayLabelByLabel = {
    timestamp: 'timestamp',
    key: target?.kind === 'timer' ? 'timer key' : 'state key',
    namespace: 'namespace',
    map_key: 'map key',
  }
  for (const label of ['timestamp', 'key', 'namespace', 'map_key']) {
    if (label === 'namespace' && isVoidSerializerClass(target?.serializer_classes?.namespace)) {
      continue
    }
    if (Object.prototype.hasOwnProperty.call(decodedKey, label)) {
      rows.push(renderDecodedPair(displayLabelByLabel[label], decodedKey[label], typeByLabel[label]))
    }
  }
  return renderDecodedBlock(rows)
}

function renderDecodedSection(value, target = null, displayId = '') {
  if (value === undefined || value === null) return ''
  if (target?.state_kind === 'LIST' && Array.isArray(value)) {
    const type = target.serializer_classes?.element
    const visible = visibleListLimit(displayId, value.length)
    const rows = value
      .slice(0, visible)
      .map((item, index) => renderDecodedPair(`#${index}`, item, type))
    if (visible < value.length) {
      rows.push(renderListMoreButton(displayId, visible, value.length))
    }
    return renderDecodedBlock(rows, 'decoded-list')
  }
  if (target?.state_kind === 'MAP' && typeof value === 'object' && !Array.isArray(value)) {
    const mapValue = Object.prototype.hasOwnProperty.call(value, 'map_value')
      ? value.map_value
      : value
    return renderDecodedBlock([renderDecodedValue(mapValue, target.serializer_classes?.map_value)])
  }
  if (Array.isArray(value)) {
    const rows = value.map((item, index) => renderDecodedPair(String(index), item))
    return renderDecodedBlock(rows)
  }
  if (typeof value === 'object' && !isRawBytesJson(value)) {
    const rows = Object.entries(value).map(([key, item]) => renderDecodedPair(key, item))
    return renderDecodedBlock(rows)
  }
  return renderDecodedBlock([renderDecodedValue(value)])
}

function visibleListLimit(displayId, total) {
  if (!displayId) return Math.min(total, LIST_VALUE_PAGE_SIZE)
  return Math.min(total, Number(state.listDisplayLimits[displayId] || LIST_VALUE_PAGE_SIZE))
}

function renderListMoreButton(displayId, visible, total) {
  if (!displayId) return ''
  const nextLimit = Math.min(total, visible + LIST_VALUE_PAGE_SIZE)
  return `
    <div class="decoded-list-more">
      <button type="button" data-list-display-id="${escapeHtml(displayId)}" data-next-limit="${nextLimit}">
        Show next ${Math.min(LIST_VALUE_PAGE_SIZE, total - visible)}
      </button>
      <span>${visible}/${total}</span>
    </div>
  `
}

function renderDecodedBlock(rows, extraClass = '') {
  if (!rows.length) return ''
  return `<div class="decoded-block ${escapeHtml(extraClass)}">${rows.join('')}</div>`
}

function renderDecodedPair(label, value, serializerClass = null) {
  const typeLabel = serializerClass ? serializerLabel(serializerClass) : ''
  return `
    <div class="decoded-row">
      <span class="decoded-label">${escapeHtml(label)}</span>
      <div class="decoded-value">${renderDecodedValue(value, serializerClass)}${typeLabel ? ` <span class="decoded-type">${escapeHtml(typeLabel)}</span>` : ''}</div>
    </div>
  `
}

function renderDecodedValue(value, serializerClass = null) {
  if (value === null || value === undefined) return '<span class="muted-text">null</span>'
  if (isRawBytesJson(value)) {
    return `${renderCode(value.b64)}${renderUtf8Pill(value.utf8)}`
  }
  if (Array.isArray(value)) {
    return renderDecodedBlock(value.map((item, index) => renderDecodedPair(String(index), item)))
  }
  if (typeof value === 'object') {
    if (Object.prototype.hasOwnProperty.call(value, 'value')) {
      return `${renderCode(value.value)}${value.type ? ` <span class="pill">${escapeHtml(truncateText(value.type, 80))}</span>` : ''}`
    }
    return renderCode(JSON.stringify(value))
  }
  const className = serializerClass || ''
  const numeric = typeof value === 'number' || /(?:Int|Long|Short|Byte|Float|Double)Serializer$/.test(className)
  return `<code class="truncated-code ${numeric ? 'decoded-number' : ''}" title="${escapeHtml(value)}">${escapeHtml(truncateText(value))}</code>`
}

function trackedTarget(item) {
  return {
    kind: item.targetKind || (item.stateKind ? 'state' : null),
    state_kind: item.stateKind,
    serializer_classes: item.serializerClasses || {},
  }
}

function isSchemaStateTarget(target) {
  return target?.kind === 'state' && Boolean(target?.state_kind) && Boolean(target?.serializer_classes)
}

function isDecodedTarget(target) {
  return (target?.kind === 'state' || target?.kind === 'timer')
    && Boolean(target?.state_kind)
    && Boolean(target?.serializer_classes)
}

function isTimerTarget(target) {
  return target?.kind === 'timer'
}

function isVoidNamespaceTarget(target) {
  return isVoidSerializerClass(target?.serializer_classes?.namespace)
}

function isVoidSerializerClass(className) {
  return className === 'org.apache.flink.runtime.state.VoidNamespaceSerializer'
}

function serializerLabel(className) {
  const simple = simpleSerializerName(className)
  const labels = {
    String: 'string',
    Int: 'int',
    Long: 'long',
    Short: 'short',
    Byte: 'byte',
    Boolean: 'boolean',
    Float: 'float',
    Double: 'double',
  }
  return labels[simple] || simple || 'raw'
}

function serializerPlaceholder(className) {
  const label = serializerLabel(className)
  if (label === 'int' || label === 'long' || label === 'short' || label === 'byte') return '42'
  if (label === 'float' || label === 'double') return '3.14'
  if (label === 'boolean') return 'true'
  if (label === 'string') return 'user-1'
  return 'use serialized bytes via API *_b64'
}

function simpleSerializerName(className = '') {
  const simple = String(className).split('.').pop() || ''
  return simple.endsWith('Serializer') ? simple.slice(0, -'Serializer'.length) : simple
}

function renderDecodeError(error) {
  return error ? `<div class="decode-error">${escapeHtml(truncateText(error))}</div>` : ''
}

function isRawBytesJson(value) {
  return value
    && typeof value === 'object'
    && Object.prototype.hasOwnProperty.call(value, 'b64')
    && Object.prototype.hasOwnProperty.call(value, 'utf8')
}

function renderUtf8Pill(value) {
  return value ? ` <span class="pill">UTF8: ${escapeHtml(truncateText(value))}</span>` : ''
}

function renderCode(value) {
  const text = value == null ? '' : String(value)
  return `<code class="truncated-code" title="${escapeHtml(text)}">${escapeHtml(truncateText(text))}</code>`
}

function truncateText(value, maxLength = MAX_VALUE_DISPLAY_LENGTH) {
  const text = value == null ? '' : String(value)
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text
}

function splitLines(value) {
  return value.split(/\r?\n/).map((line) => line.trim()).filter(Boolean)
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
}

function showView(view) {
  document.querySelectorAll('.nav-button').forEach((button) => {
    button.classList.toggle('active', button.dataset.view === view)
  })
  $('inspect-view').classList.toggle('hidden', view !== 'inspect')
  $('datasource-view').classList.toggle('hidden', view !== 'datasource')
  $('page-title').textContent = view === 'inspect' ? 'Inspect' : 'Datasource'
  $('page-subtitle').textContent = view === 'inspect'
    ? 'Read raw key/value rows from a Cobble Flink snapshot.'
    : 'Choose latest or a concrete checkpoint/snapshot.'
}

document.querySelectorAll('.nav-button').forEach((button) => {
  button.addEventListener('click', () => showView(button.dataset.view))
})
$('refresh-button').addEventListener('click', refresh)
$('open-new-path-button').addEventListener('click', openNewPath)
$('operator-select').addEventListener('change', switchOperator)
$('inspect-target').addEventListener('change', refreshTargetControls)
$('inspect-button').addEventListener('click', () => runInspect())
$('lookup-button').addEventListener('click', () => runLookup())
$('clear-lookup-button').addEventListener('click', () => {
  clearTrackedLookups()
  renderLookupResult()
  scheduleInspectAutoRefresh()
})
$('next-page-button').addEventListener('click', () => runInspect({ next: true }))
$('prev-page-button').addEventListener('click', () => runScan('previous'))
$('scan-tab').addEventListener('click', () => switchInspectMode('scan'))
$('lookup-tab').addEventListener('click', () => switchInspectMode('lookup'))
$('auto-refresh-enabled').addEventListener('change', updateInspectAutoRefresh)
$('auto-refresh-seconds').addEventListener('change', updateInspectAutoRefresh)
document.addEventListener('click', closeRowActionPopover)
window.addEventListener('resize', closeRowActionPopover)
window.addEventListener('scroll', closeRowActionPopover, true)
$('new-source-path').addEventListener('keydown', (event) => {
  if (event.key === 'Enter') openNewPath()
})
;['bucket', 'prefix', 'state-key', 'namespace', 'map-key', 'limit', 'columns', 'inspect-target'].forEach((id) => {
  $(id).addEventListener('change', invalidatePagination)
})

switchInspectMode('scan')
refresh()
