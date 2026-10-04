const assert = require('node:assert/strict')
const fs = require('node:fs')
const vm = require('node:vm')

const [base, source, firstId, ready, advanced] = process.argv.slice(2)
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const elements = new Map()
function element() {
  const classes = new Set()
  return {
    value: '', checked: false, disabled: false, textContent: '', dataset: {}, children: [], style: {},
    get innerHTML() { return this.html || '' },
    set innerHTML(value) { this.html = value; this.children = [] },
    classList: {
      add: (name) => classes.add(name), remove: (name) => classes.delete(name),
      contains: (name) => classes.has(name),
      toggle: (name, on) => on ? classes.add(name) : classes.delete(name),
    },
    addEventListener() {}, setAttribute() {}, removeAttribute() {}, focus() {}, remove() {},
    appendChild(child) { this.children.push(child) },
    querySelectorAll: () => [], querySelector: () => element(), closest: () => null,
  }
}
const get = (id) => {
  if (!elements.has(id)) elements.set(id, element())
  return elements.get(id)
}
let interval
const calls = []
const context = vm.createContext({
  document: {
    getElementById: get, createElement: element, querySelectorAll: () => [],
    querySelector: () => element(), addEventListener() {},
  },
  window: { addEventListener() {} }, console, TextEncoder, TextDecoder, atob, btoa,
  setInterval: (callback) => { interval = callback; return 1 },
  clearInterval: () => { interval = null },
  fetch: async (url, options) => {
    calls.push({ url, options })
    return fetch(new URL(url, base), options)
  },
})

async function main() {
  const response = await fetch(`${base}/app.js`)
  assert.equal(response.status, 200)
  const script = await response.text()
  vm.runInContext(`${script}\nglobalThis.api = { state, parseExactJson, requestJson, checkpointParam,
    post, request, replaceSession, withSessionRecovery, refresh, runScan, runLookup, trackScanItem,
    trackIdentity, renderSinkExpandedValue, renderSemanticTableValue, typedField,
    scheduleInspectAutoRefresh, renderSnapshots };`, context)
  const api = context.api
  const scalar = (part) => part.fields?.[0]?.value || part
  const key = (row) => String(scalar(row.decoded_key).value)
  const label = (row) => scalar(row.decoded_columns[0]).value
  const assertHealthy = () => assert.equal(api.state.errorSource, null, get('alert').textContent)

  const escaped = 'digits 9007199254740993, quote " and slash \\ and newline\n'
  const mixed = api.parseExactJson(`{"big":9007199254740993,"min":-9223372036854775808,"max":9223372036854775807,"safe":42,"bool":true,"float":1.25,"exp":1e3,"text":${JSON.stringify(escaped)}}`)
  assert.equal(mixed.big, '9007199254740993')
  assert.equal(mixed.min, '-9223372036854775808')
  assert.equal(mixed.max, '9223372036854775807')
  assert.equal(mixed.safe, 42)
  assert.equal(mixed.bool, true)
  assert.equal(mixed.float, '1.25')
  assert.equal(mixed.exp, '1e3')
  const decimals = api.parseExactJson('{"nested":[123456789012345678.123456,-0.000000000000000001,1e-400]}')
  assert.deepEqual(Array.from(decimals.nested), ['123456789012345678.123456', '-0.000000000000000001', '1e-400'])
  assert.equal(mixed.text, escaped)
  const checkpointJson = api.requestJson({ source: escaped, checkpoint: api.checkpointParam('9223372036854775807') })
  assert.match(checkpointJson, /"checkpoint":9223372036854775807[},]/)
  assert.equal(api.parseExactJson(checkpointJson).source, escaped)
  api.state.meta = { selected_checkpoint: '9007199254740993', selected_checkpoint_id: '9007199254740993' }
  api.state.snapshots = ['9007199254740992', '9007199254740993'].map((id) => ({ id, directory: id }))
  api.renderSnapshots()
  const snapshotRows = get('snapshots-body').children.slice(-2)
  assert.ok(!snapshotRows[0].innerHTML.includes('check-mark'))
  assert.ok(snapshotRows[1].innerHTML.includes('check-mark'))

  await api.replaceSession('latest', '', source)
  get('limit').value = '50'
  get('bucket').value = 'all'
  await api.runScan()
  assertHealthy()
  const rows = api.state.lastScanData.scan.items
  assert.equal(rows.length, 4)
  const ids = ['9007199254740992', '9007199254740993', '-9223372036854775808', '9223372036854775807']
  for (const id of ids) {
    const row = rows.find((candidate) => key(candidate) === id)
    assert.ok(row, id)
    assert.match(api.renderSinkExpandedValue(row.decoded_key.fields[0]), new RegExp(id))
    const result = await api.post(`/api/v1/sessions/${api.state.sessionId}/lookup`, {
      target_id: api.state.meta.inspect_targets[0].id,
      keys: [{ kind: 'sink', fields: [api.typedField({ name: 'id', logical_type: 'BIGINT' }, key(row))] }],
    })
    assert.equal(key(result.rows[0]), id)
    assert.equal(label(result.rows[0]), label(row))
  }
  const even = rows.find((row) => key(row) === ids[0])
  const odd = rows.find((row) => key(row) === ids[1])
  const bytes = scalar(even.decoded_columns[1])
  const nil = scalar(odd.decoded_columns[1])
  assert.equal(bytes.kind, 'RAW')
  assert.match(api.renderSinkExpandedValue(even.decoded_columns[1].fields[0]), /AAEA/)
  assert.match(api.renderSemanticTableValue(bytes, ''), /AAEA/)
  assert.equal(nil.kind, 'SCALAR')
  assert.equal(nil.value, undefined)
  assert.match(api.renderSinkExpandedValue(odd.decoded_columns[1].fields[0]), /null/)
  assert.match(api.renderSemanticTableValue(nil, ''), /null/)
  assert.match(api.renderSemanticTableValue({ kind: 'RAW', raw_b64: '' }, ''), /empty bytes/)

  for (const row of [even, odd]) {
    const scan = api.state.lastScanContext
    api.trackScanItem(api.trackIdentity(row.bucket, row.key_b64, scan.targetId, scan.columns))
  }
  api.state.columnSelections[api.state.meta.inspect_targets[0].id] = [1, 0]
  const tracked = Array.from(api.state.trackedLookups)
  fs.writeFileSync(ready, 'ready')
  while (!fs.existsSync(advanced)) await sleep(20)
  const secondId = fs.readFileSync(advanced, 'utf8')
  await api.refresh()
  assertHealthy()
  assert.equal(String(api.state.meta.selected_checkpoint_id), secondId)
  assert.equal(api.state.trackedLookups.length, 2)
  assert.equal(api.state.trackedLookups[0], tracked[0])
  assert.deepEqual(Array.from(api.state.columnSelections[api.state.meta.inspect_targets[0].id]), [1, 0])
  assert.equal(scalar(api.state.trackedLookups[0].decodedColumns[0]).value, 'even-second')
  assert.equal(scalar(api.state.trackedLookups[1].decodedColumns[0]).value, 'odd-second')

  // Delete and idle timeout are session loss, not checkpoint loss. Exact remains pinned.
  await api.replaceSession(firstId, '', source)
  const pinned = () => String(api.state.meta.selected_checkpoint_id)
  let oldSession = api.state.sessionId
  await api.request(`/api/v1/sessions/${oldSession}`, { method: 'DELETE' })
  await api.withSessionRecovery(() => api.request(`/api/v1/sessions/${api.state.sessionId}`))
  assert.notEqual(api.state.sessionId, oldSession)
  assert.equal(pinned(), firstId)
  assert.equal(api.state.selectedLatest, false)
  oldSession = api.state.sessionId
  await sleep(2300)
  await api.withSessionRecovery(() => api.request(`/api/v1/sessions/${api.state.sessionId}`))
  assert.notEqual(api.state.sessionId, oldSession)
  assert.equal(pinned(), firstId)

  await api.replaceSession('latest', '', source)
  oldSession = api.state.sessionId
  await api.request(`/api/v1/sessions/${oldSession}`, { method: 'DELETE' })
  await api.withSessionRecovery(() => api.request(`/api/v1/sessions/${api.state.sessionId}`))
  assert.notEqual(api.state.sessionId, oldSession)
  assert.equal(pinned(), secondId)
  assert.equal(api.state.selectedLatest, true)

  // A stale latest selection exercises the actual auto-refresh callback against newer data.
  await api.replaceSession(firstId, '', source)
  await api.runScan()
  const oldRow = api.state.lastScanData.scan.items.find((row) => key(row) === ids[1])
  const scan = api.state.lastScanContext
  api.trackScanItem(api.trackIdentity(oldRow.bucket, oldRow.key_b64, scan.targetId, scan.columns))
  api.state.selectedLatest = true
  api.state.inspectAutoRefreshEnabled = true
  api.scheduleInspectAutoRefresh()
  await interval()
  assertHealthy()
  assert.equal(pinned(), secondId)
  assert.equal(api.state.trackedLookups.length, 1)
  assert.equal(scalar(api.state.trackedLookups[0].decodedColumns[0]).value, 'odd-second')
  assert.ok(calls.some((call) => call.url.includes('/lookup')))
  console.log('SERVED_JS_HTTP_REGRESSION_PASS')
}

const watchdog = setTimeout(() => { console.error('probe timed out'); process.exit(137) }, 55000)
main().then(() => { clearTimeout(watchdog); process.exit(0) }, (error) => {
  console.error(error)
  process.exit(1)
})
