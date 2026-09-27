const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const test = require('node:test')
const vm = require('node:vm')

const script = fs.readFileSync(
  path.join(__dirname, '../../main/resources/web/app.js'), 'utf8',
)
const start = script.indexOf('function scanRowActions(')
const end = script.indexOf('\nfunction renderActionMenu(', start)
assert.ok(start >= 0 && end > start)
const actions = vm.runInNewContext(
  `${script.slice(start, end)}\n({ scanRowActions, rawCopyActions })`,
  { rawCopyValue: (value) => JSON.stringify(value) },
)

test('semantic Table row omits copy raw value when no bytes were read', () => {
  const rowActions = actions.scanRowActions(
    { key_b64: 'YQ==', columns: [], value: null },
    null,
    { allowsColumns: true },
    'tracked',
  )
  assert.deepEqual(Array.from(rowActions, (action) => action.label), ['Track', 'Copy raw key'])
})

test('raw values remain copyable, including an empty byte value', () => {
  assert.deepEqual(
    Array.from(actions.rawCopyActions('YQ==', [{ b64: 'dg==' }]), (action) => action.label),
    ['Copy raw key', 'Copy raw value'],
  )
  assert.deepEqual(
    Array.from(actions.rawCopyActions('YQ==', ''), (action) => action.label),
    ['Copy raw key', 'Copy raw value'],
  )
})
