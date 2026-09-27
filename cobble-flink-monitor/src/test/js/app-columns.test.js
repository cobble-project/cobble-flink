const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const test = require('node:test')
const vm = require('node:vm')

const script = fs.readFileSync(
  path.join(__dirname, '../../main/resources/web/app.js'), 'utf8',
)

function section(first, next) {
  const start = script.indexOf(`function ${first}(`)
  const end = script.indexOf(`\nfunction ${next}(`, start)
  assert.ok(start >= 0 && end > start)
  return script.slice(start, end)
}

function element() {
  const classes = new Set(['hidden'])
  return {
    value: '',
    innerHTML: '',
    textContent: '',
    disabled: false,
    classList: {
      add: (name) => classes.add(name),
      remove: (name) => classes.delete(name),
      toggle: (name, enabled) => enabled ? classes.add(name) : classes.delete(name),
      contains: (name) => classes.has(name),
    },
    setAttribute() {},
    focus() {},
  }
}

function pickerFixture() {
  const elements = Object.create(null)
  const state = {
    columnSelections: Object.create(null),
    observedRawColumns: Object.create(null),
    columnDraft: null,
  }
  let target = {
    id: 'table', kind: 'sink', allows_columns: true,
    value_fields: [
      { name: 'first', logical_type: 'STRING', structured_column_index: 0 },
      { name: 'third', logical_type: 'BIGINT', structured_column_index: 2 },
    ],
  }
  let invalidations = 0
  const refreshes = []
  const context = {
    state,
    activeTarget: () => target,
    isSinkTarget: (value) => value?.kind === 'sink',
    $: (id) => elements[id] ||= element(),
    escapeHtml: (value) => String(value),
    invalidatePagination: () => { invalidations += 1 },
    switchInspectMode: (mode) => { refreshes.push(mode) },
    runScan: () => { refreshes.push('scan-request') },
  }
  const names = [
    'columnSelection', 'projectionKey', 'columnOptions', 'columnSummary',
    'renderColumnPicker', 'openColumnPicker', 'closeColumnPicker',
    'applyColumnPicker', 'parseRawColumnNumber', 'addRawColumn', 'observeRawColumns',
  ]
  const picker = vm.runInNewContext(
    `${section('columnSelection', 'typedGroupFields')}\n({ ${names.join(', ')} })`,
    context,
  )
  return {
    picker, state, elements,
    setTarget: (value) => { target = value },
    invalidations: () => invalidations,
    refreshes,
  }
}

test('Table choices keep physical indexes and requested order', () => {
  const { picker, state } = pickerFixture()
  assert.deepEqual(
    Array.from(picker.columnOptions({
      kind: 'sink', value_fields: [
        { name: 'third', structured_column_index: 2 },
        { name: 'first', structured_column_index: 0 },
      ],
    }), (field) => [field.name, field.index]),
    [['third', 2], ['first', 0]],
  )
  state.columnSelections.table = [2, 0]
  assert.deepEqual(Array.from(picker.columnSelection()), [2, 0])
  assert.notEqual(picker.projectionKey([2, 0]), picker.projectionKey([0, 2]))
})

test('empty draft cannot apply, All restores null, and targets stay isolated', () => {
  const fixture = pickerFixture()
  const { picker, state, elements } = fixture
  state.columnSelections.table = [2]
  picker.openColumnPicker()
  state.columnDraft = []
  picker.renderColumnPicker()
  assert.equal(elements['columns-apply'].disabled, true)
  picker.applyColumnPicker()
  assert.deepEqual(Array.from(state.columnSelections.table), [2])
  assert.deepEqual(fixture.refreshes, [])
  state.columnDraft = null
  picker.applyColumnPicker()
  assert.equal(picker.columnSelection(), null)
  assert.equal(fixture.invalidations(), 1)
  assert.deepEqual(fixture.refreshes, ['scan', 'scan-request'])
  fixture.setTarget({ id: '__proto__', kind: 'raw', allows_columns: true })
  assert.equal(picker.columnSelection(), null)
  state.columnSelections.__proto__ = [7]
  assert.deepEqual(Array.from(picker.columnSelection()), [7])
})

test('applying a projection refreshes the scan with the new selection', () => {
  const { picker, state, refreshes } = pickerFixture()
  state.columnDraft = [2, 0]
  picker.applyColumnPicker()
  assert.deepEqual(Array.from(picker.columnSelection()), [2, 0])
  assert.deepEqual(refreshes, ['scan', 'scan-request'])
})

test('raw candidates only reflect observed or explicitly chosen indexes', () => {
  const fixture = pickerFixture()
  const { picker, state } = fixture
  fixture.setTarget({ id: 'raw', kind: 'raw', allows_columns: true })
  for (const invalid of ['-1', '1.5', '1e3', '2147483648', '']) {
    assert.throws(() => picker.parseRawColumnNumber(invalid))
  }
  assert.equal(picker.parseRawColumnNumber('2147483647'), 2147483647)
  picker.observeRawColumns({ id: 'raw', kind: 'raw', allows_columns: true },
    [{ columns: [{}, {}] }], null)
  picker.observeRawColumns({ id: 'raw', kind: 'raw', allows_columns: true },
    [{ columns: [{}] }], [5])
  assert.deepEqual(Array.from(state.observedRawColumns.raw), [0, 1, 5])
  picker.addRawColumn('7')
  assert.deepEqual(Array.from(state.columnDraft), [7])
  assert.deepEqual(Array.from(state.observedRawColumns.raw), [0, 1, 5, 7])
})

test('Track owns its projection snapshot and groups projections separately', () => {
  const state = {
    lastScanContext: {
      targetId: 'table', targetLabel: 'table', columns: [2, 0],
      allowsColumns: true, targetKind: 'sink',
    },
    lastScanData: { scan: { items: [{ bucket: 0, key_b64: 'YQ==', columns: [] }] } },
    trackedLookups: [],
  }
  const names = ['trackScanItem', 'trackIdentity', 'scanItems', 'groupedTrackedLookups']
  const tracked = vm.runInNewContext(
    `${section('trackScanItem', 'removeTrackedLookup')}\n({ ${names.join(', ')} })`,
    { state, projectionKey: (columns) => columns === null ? '*' : JSON.stringify(columns),
      switchInspectMode() {}, renderLookupResult() {} },
  )
  tracked.trackScanItem(tracked.trackIdentity(0, 'YQ==', 'table', [2, 0]))
  state.lastScanContext.columns.push(1)
  assert.deepEqual(Array.from(state.trackedLookups[0].columns), [2, 0])
  state.trackedLookups.push({ targetId: 'table', columns: [0, 2] })
  assert.equal(tracked.groupedTrackedLookups().length, 2)
})

test('raw cells and Table headers follow physical projection order', () => {
  const renderColumns = vm.runInNewContext(
    `${section('renderColumns', 'renderSinkColumns')}\nrenderColumns`,
    { renderCode: (value) => value, renderUtf8Pill: () => '' },
  )
  assert.match(renderColumns([{ b64: 'dg==' }], [5]), /<strong>5<\/strong>/)
  const fields = vm.runInNewContext(
    `${section('projectedSinkValueFields', 'renderSinkScanHeader')}\nprojectedSinkValueFields`,
  )
  const values = [
    { name: 'first', structured_column_index: 0 },
    { name: 'third', structured_column_index: 2 },
  ]
  assert.deepEqual(Array.from(fields(values, [2, 0]), (field) => field.name), ['third', 'first'])
  assert.equal(fields(values, null).length, 2)
})

test('target changes invalidate the visible scan and pager', () => {
  const calls = []
  const refresh = vm.runInNewContext(
    `${section('refreshTargetControls', 'trackScanItem')}\nrefreshTargetControls`,
    {
      renderInspectTargets: () => calls.push('targets'),
      renderBucketRange: () => calls.push('bucket'),
      invalidatePagination: () => calls.push('invalidate'),
    },
  )
  refresh()
  assert.deepEqual(calls, ['targets', 'bucket', 'invalidate'])
})
