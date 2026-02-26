import './styles.css'

const root = document.getElementById('app')

const PAGE_SIZE = 8
const NULL_SENTINEL = '__PUPPYDB_NULL__'

const state = {
  theme: 'dark',
  route: 'connections',
  workspaceTab: 'explorer',
  loadingConnections: false,
  connectionSearch: '',
  connectionPage: 1,
  connectionTotal: 0,
  connectionTotalPages: 1,
  connections: [],
  activeConnection: null,
  loadingObjects: false,
  objectTree: {
    schemas: [],
    bySchema: {},
  },
  selectedSchema: '',
  tableListSearch: '',
  tableOverview: [],
  loadingTableOverview: false,
  tableOverviewError: '',
  tableTabs: [],
  activeTableTab: '',
  tableStateByTab: {},
  queryEditor: '-- Enter SQL query here\nSELECT 1;',
  queryResult: {
    columns: [],
    rows: [],
    rowsAffected: 0,
    message: '',
    durationMs: 0,
  },
  queryResultColumnWidths: {},
  runningQuery: false,
  snippets: [],
  history: [],
  queryPanel: 'snippets',
  manager: {
    selectedID: '',
    search: '',
    busy: false,
    title: 'Add New Connection',
    unsaved: false,
    form: defaultConnectionForm(),
  },
  openingConnectionID: '',
  rowModal: {
    open: false,
    busy: false,
    loading: false,
    mode: 'insert',
    tabId: '',
    fields: {},
    nulls: {},
    columns: [],
    keyColumn: '',
    keyValue: '',
  },
  cellViewer: {
    open: false,
    tabId: '',
    rowIndex: -1,
    colIndex: -1,
    value: '',
    prettyValue: '',
    isJSON: false,
  },
  inlineCell: {
    open: false,
    tabId: '',
    rowIndex: -1,
    colIndex: -1,
  },
  structureModal: {
    open: false,
    loading: false,
    schema: '',
    table: '',
    columns: [],
    error: '',
  },
  tableActionBusy: '',
  resizing: {
    active: false,
    target: '',
    tabId: '',
    column: '',
    startX: 0,
    startWidth: 0,
  },
  toast: null,
  toastTimer: null,
}

const demoConnections = [
  {
    id: 'demo-pg',
    name: 'PostgreSQL Production',
    host: 'db.prod.example.com',
    port: 5432,
    type: 'POSTGRES',
    mode: 'DIRECT',
    status: 'Live',
    subline: 'v14.5 • Analytics',
  },
  {
    id: 'demo-mysql',
    name: 'MySQL Staging',
    host: '192.168.1.45',
    port: 3306,
    type: 'MYSQL',
    mode: 'SSH TUNNEL',
    status: 'Inactive',
    subline: 'v8.0 • CRM Service',
  },
  {
    id: 'demo-mongo',
    name: 'MongoDB Cluster',
    host: 'cluster0.mongodb.net',
    port: 27017,
    type: 'MONGO',
    mode: 'DIRECT',
    status: 'Live',
    subline: 'Atlas • Logs App',
  },
  {
    id: 'demo-redis',
    name: 'Redis Cache',
    host: 'localhost',
    port: 6379,
    type: 'REDIS',
    mode: 'DIRECT',
    status: 'Inactive',
    subline: 'v6.2 • Sessions',
  },
]

let searchTimer = null

function defaultConnectionForm() {
  return {
    id: '',
    mode: 'direct',
    name: '',
    type: 'mysql',
    useConnString: false,
    connString: '',
    host: '127.0.0.1',
    port: '3306',
    database: '',
    schema: '',
    username: '',
    password: '',
    sshHost: '',
    sshPort: '22',
    sshUser: '',
    sshAuthType: 'password',
    sshPassword: '',
    sshKeyFile: '',
    sshPassphrase: '',
  }
}

function appApi() {
  return window.go?.main?.App ?? null
}

function hasBackend() {
  return !!appApi()
}

async function callApi(method, ...args) {
  const api = appApi()
  if (!api || typeof api[method] !== 'function') {
    throw new Error('Desktop backend is not available in browser preview mode')
  }
  return api[method](...args)
}

function esc(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;')
}

function dbTypeLabel(type) {
  const key = String(type || '').toLowerCase()
  if (key === 'postgres' || key === 'postgresql') {
    return 'PostgreSQL'
  }
  if (key === 'mysql') {
    return 'MySQL'
  }
  if (key === 'mongo' || key === 'mongodb') {
    return 'MongoDB'
  }
  if (key === 'redis') {
    return 'Redis'
  }
  return String(type || '').toUpperCase()
}

function dbTypeShort(type) {
  const key = String(type || '').toLowerCase()
  if (key.includes('post')) {
    return 'PG'
  }
  if (key.includes('mysql')) {
    return 'MY'
  }
  if (key.includes('mongo')) {
    return 'MG'
  }
  if (key.includes('redis')) {
    return 'RD'
  }
  return 'DB'
}

function dbTypeClass(type) {
  const key = String(type || '').toLowerCase()
  if (key.includes('post')) {
    return 'db-postgres'
  }
  if (key.includes('mysql')) {
    return 'db-mysql'
  }
  if (key.includes('mongo')) {
    return 'db-mongo'
  }
  if (key.includes('redis')) {
    return 'db-redis'
  }
  return 'db-generic'
}

function dbTypeGlyph(type) {
  const key = String(type || '').toLowerCase()
  if (key.includes('post')) {
    return 'PG'
  }
  if (key.includes('mysql')) {
    return 'MY'
  }
  if (key.includes('mongo')) {
    return 'MG'
  }
  if (key.includes('redis')) {
    return 'RD'
  }
  return 'DB'
}

function isSQLType(type) {
  const key = String(type || '').toLowerCase()
  return key === 'postgres' || key === 'postgresql' || key === 'mysql'
}

function activeType() {
  return String(state.activeConnection?.type || '').toLowerCase()
}

function formatTime(dt) {
  if (!dt) {
    return '-'
  }
  const date = new Date(dt)
  if (Number.isNaN(date.getTime())) {
    return '-'
  }
  return date.toLocaleString()
}

function notify(message, error = false) {
  state.toast = { message, error }
  if (state.toastTimer) {
    clearTimeout(state.toastTimer)
  }
  state.toastTimer = setTimeout(() => {
    state.toast = null
    render()
  }, 3000)
  render()
}

function tableTabId(schema, table) {
  return `${schema || '-'}::${table}`
}

function tableActionKey(action, schema, table) {
  return `${action}:${schema || '-'}::${table || ''}`
}

function safeFilePart(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '_')
    .replace(/^[_\-.]+|[_\-.]+$/g, '')
}

function tableListQuery() {
  return String(state.tableListSearch || '').trim().toLowerCase()
}

function tableListMatch(name, schema = '') {
  const query = tableListQuery()
  if (!query) {
    return true
  }
  const haystack = `${String(schema || '')} ${String(name || '')}`.toLowerCase()
  return haystack.includes(query)
}

function filteredTableOverviewItems() {
  if (!Array.isArray(state.tableOverview)) {
    return []
  }
  return state.tableOverview.filter((item) => tableListMatch(item?.name, item?.schema || state.selectedSchema))
}

function cellTextValue(value) {
  if (value == null) {
    return 'NULL'
  }
  if (typeof value === 'string') {
    return value
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value)
    } catch {
      return String(value)
    }
  }
  return String(value)
}

function cellPreviewValue(value, maxLen = 96) {
  const text = cellTextValue(value)
  if (text.length <= maxLen) {
    return text
  }
  return `${text.slice(0, Math.max(1, maxLen - 1))}…`
}

function shouldShowCellViewer(value) {
  const text = cellTextValue(value)
  const trimmed = text.trim()
  const jsonLike = (trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))
  return text.length > 80 || /[\r\n\t]/.test(text) || jsonLike
}

function decodeJSONValue(value) {
  const raw = cellTextValue(value)
  const trimmed = raw.trim()
  if (!trimmed || !(trimmed.startsWith('{') || trimmed.startsWith('['))) {
    return { isJSON: false, pretty: '' }
  }
  try {
    const parsed = JSON.parse(trimmed)
    return {
      isJSON: true,
      pretty: JSON.stringify(parsed, null, 2),
    }
  } catch {
    return { isJSON: false, pretty: '' }
  }
}

function clearInlineCell() {
  state.inlineCell = {
    open: false,
    tabId: '',
    rowIndex: -1,
    colIndex: -1,
  }
}

function closeCellViewer() {
  state.cellViewer = {
    open: false,
    tabId: '',
    rowIndex: -1,
    colIndex: -1,
    value: '',
    prettyValue: '',
    isJSON: false,
  }
}

function clearCellInteractions(tabId = '') {
  if (!tabId || state.inlineCell.tabId === tabId) {
    clearInlineCell()
  }
  if (!tabId || state.cellViewer.tabId === tabId) {
    closeCellViewer()
  }
}

function focusInlineCellEditor() {
  if (!state.inlineCell.open) {
    return
  }
  const rowIndex = Number(state.inlineCell.rowIndex)
  const colIndex = Number(state.inlineCell.colIndex)
  requestAnimationFrame(() => {
    const selector = `[data-cell-editor=\"true\"][data-row-index=\"${rowIndex}\"][data-col-index=\"${colIndex}\"]`
    const input = root.querySelector(selector)
    if (input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement) {
      input.focus()
      input.select()
    }
  })
}

function currentTableState() {
  return state.tableStateByTab[state.activeTableTab] || null
}

function blankTableState(schema, table) {
  return {
    schema,
    table,
    page: 1,
    limit: 50,
    filterDraft: '',
    filter: '',
    sortDraft: '',
    sort: '',
    sortColumn: '',
    sortDirection: '',
    columns: [],
    columnWidths: {},
    columnMeta: [],
    rows: [],
    totalRows: 0,
    from: 0,
    to: 0,
    hasNext: false,
    loading: false,
    error: '',
    selectedRow: -1,
  }
}

function loadingMarkup(message) {
  return `
    <div class="loading-state">
      <span class="loading-spinner" aria-hidden="true"></span>
      <span>${esc(message)}</span>
    </div>
  `
}

function ensureColumnWidths(columns, current) {
  const output = { ...(current || {}) }
  const set = new Set(columns || [])
  for (const column of columns || []) {
    if (!output[column]) {
      const guess = Math.max(130, Math.min(360, column.length * 12 + 42))
      output[column] = guess
    }
  }
  for (const key of Object.keys(output)) {
    if (!set.has(key)) {
      delete output[key]
    }
  }
  return output
}

function gridTemplateFromColumns(columns, widths) {
  if (!columns || !columns.length) {
    return '140px'
  }
  return columns
    .map((column) => {
      const width = Math.max(100, Math.min(920, Number(widths?.[column] || 180)))
      return `${width}px`
    })
    .join(' ')
}

function applyActiveTableGridTemplate(tabId) {
  const tableState = state.tableStateByTab[tabId]
  if (!tableState) {
    return
  }
  const template = gridTemplateFromColumns(tableState.columns, tableState.columnWidths)
  root.querySelectorAll('[data-table-grid="active"]').forEach((node) => {
    node.style.gridTemplateColumns = template
  })
}

function applyQueryGridTemplate() {
  const template = gridTemplateFromColumns(state.queryResult.columns, state.queryResultColumnWidths)
  root.querySelectorAll('[data-query-grid="active"]').forEach((node) => {
    node.style.gridTemplateColumns = template
  })
}

function normalizeColumnMeta(columns, metaItems) {
  const byName = new Map()
  for (const raw of metaItems || []) {
    const name = String(raw?.name || '').trim()
    if (!name) {
      continue
    }
    const enumValues = Array.isArray(raw?.enumValues) ? raw.enumValues : Array.isArray(raw?.enum_values) ? raw.enum_values : []
    byName.set(name, {
      name,
      dataType: String(raw?.dataType || raw?.data_type || 'text').toLowerCase(),
      nullable: raw?.nullable !== false,
      defaultValue: String(raw?.defaultValue || raw?.default_value || ''),
      isPrimary: !!(raw?.isPrimary || raw?.is_primary),
      enumValues: enumValues.map((v) => String(v)),
    })
  }

  return (columns || []).map((column) => {
    if (byName.has(column)) {
      return byName.get(column)
    }
    const guess = String(column || '').toLowerCase()
    let dataType = 'text'
    if (guess.endsWith('_id') || guess === 'id') {
      dataType = 'uuid'
    } else if (guess.includes('date') || guess.includes('time')) {
      dataType = 'timestamp'
    } else if (guess.includes('count') || guess.includes('total') || guess.includes('amount') || guess.includes('price')) {
      dataType = 'numeric'
    }
    return {
      name: column,
      dataType,
      nullable: true,
      defaultValue: '',
      isPrimary: false,
      enumValues: [],
    }
  })
}

async function loadTableColumnMeta(tabId = state.activeTableTab) {
  const tab = state.tableTabs.find((item) => item.id === tabId)
  const tableState = state.tableStateByTab[tabId]
  if (!tab || !tableState || !tableState.columns.length) {
    return []
  }

  if (tableState.columnMeta?.length) {
    return tableState.columnMeta
  }

  if (!hasBackend()) {
    tableState.columnMeta = normalizeColumnMeta(tableState.columns, [])
    return tableState.columnMeta
  }

  try {
    const meta = await callApi('DescribeTableColumnsActive', tab.schema, tab.table)
    tableState.columnMeta = normalizeColumnMeta(tableState.columns, meta || [])
  } catch (err) {
    tableState.columnMeta = normalizeColumnMeta(tableState.columns, [])
    notify(String(err?.message || err), true)
  }
  return tableState.columnMeta
}

function rowModalMetaByName() {
  const map = {}
  for (const column of state.rowModal.columns || []) {
    map[column.name] = column
  }
  return map
}

function rowModalColumns(tabId, fallbackColumns = []) {
  const tableState = state.tableStateByTab[tabId]
  if (tableState?.columnMeta?.length) {
    return tableState.columnMeta
  }
  return normalizeColumnMeta(fallbackColumns, [])
}

function isNullLikeValue(value) {
  return String(value ?? '').trim().toUpperCase() === 'NULL'
}

function isTypeIn(dataType, parts) {
  const dt = String(dataType || '').toLowerCase()
  return parts.some((part) => dt.includes(part))
}

function isNumericType(dataType) {
  return isTypeIn(dataType, ['int', 'numeric', 'decimal', 'float', 'double', 'real', 'serial', 'money'])
}

function isIntegerType(dataType) {
  return isTypeIn(dataType, ['smallint', 'integer', 'bigint', 'int', 'serial']) && !isTypeIn(dataType, ['point', 'interval'])
}

function isDateType(dataType) {
  return isTypeIn(dataType, ['date']) && !isTypeIn(dataType, ['datetime', 'timestamp'])
}

function isDateTimeType(dataType) {
  return isTypeIn(dataType, ['timestamp', 'datetime'])
}

function isTimeType(dataType) {
  return isTypeIn(dataType, ['time']) && !isDateTimeType(dataType)
}

function isBooleanType(dataType) {
  return isTypeIn(dataType, ['bool', 'tinyint(1)'])
}

function isUUIDType(dataType) {
  return isTypeIn(dataType, ['uuid'])
}

function isJSONType(dataType) {
  return isTypeIn(dataType, ['json'])
}

function toDateTimeLocalInput(value) {
  const raw = String(value || '').trim()
  if (!raw || raw.toUpperCase() === 'NULL') {
    return ''
  }
  const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T')
  const date = new Date(normalized)
  if (Number.isNaN(date.getTime())) {
    return ''
  }
  const yyyy = date.getFullYear()
  const mm = String(date.getMonth() + 1).padStart(2, '0')
  const dd = String(date.getDate()).padStart(2, '0')
  const hh = String(date.getHours()).padStart(2, '0')
  const min = String(date.getMinutes()).padStart(2, '0')
  return `${yyyy}-${mm}-${dd}T${hh}:${min}`
}

function toDateInput(value) {
  const raw = String(value || '').trim()
  if (!raw || raw.toUpperCase() === 'NULL') {
    return ''
  }
  const date = new Date(raw)
  if (Number.isNaN(date.getTime())) {
    return ''
  }
  const yyyy = date.getFullYear()
  const mm = String(date.getMonth() + 1).padStart(2, '0')
  const dd = String(date.getDate()).padStart(2, '0')
  return `${yyyy}-${mm}-${dd}`
}

function nowValueForType(dataType) {
  const now = new Date()
  if (isDateType(dataType)) {
    return toDateInput(now.toISOString())
  }
  if (isDateTimeType(dataType)) {
    return toDateTimeLocalInput(now.toISOString())
  }
  if (isTimeType(dataType)) {
    return `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`
  }
  return ''
}

function normalizeValueForType(rawValue, dataType) {
  const value = String(rawValue ?? '')
  if (!value || isNullLikeValue(value)) {
    return ''
  }
  if (isDateTimeType(dataType)) {
    return toDateTimeLocalInput(value)
  }
  if (isDateType(dataType)) {
    return toDateInput(value)
  }
  if (isTimeType(dataType)) {
    const match = value.match(/^(\d{2}:\d{2}(?::\d{2})?)/)
    return match ? match[1] : value
  }
  if (isBooleanType(dataType)) {
    const clean = value.trim().toLowerCase()
    if (['1', 't', 'true', 'yes', 'y'].includes(clean)) {
      return 'true'
    }
    if (['0', 'f', 'false', 'no', 'n'].includes(clean)) {
      return 'false'
    }
  }
  return value
}

function typeBadge(dataType) {
  const value = String(dataType || 'text').trim()
  return value ? value.toUpperCase() : 'TEXT'
}

function rowFieldIcon(meta) {
  const name = String(meta?.name || '').toLowerCase()
  const dt = String(meta?.dataType || '').toLowerCase()
  if (isUUIDType(dt) || name === 'id' || name.endsWith('_id')) {
    return 'ID'
  }
  if (isNumericType(dt)) {
    return '#'
  }
  if (isDateType(dt) || isDateTimeType(dt) || isTimeType(dt)) {
    return 'DT'
  }
  if (isBooleanType(dt)) {
    return 'TF'
  }
  if (isJSONType(dt)) {
    return '{}'
  }
  if ((meta?.enumValues || []).length) {
    return 'EN'
  }
  return 'Aa'
}

function normalizeModalFieldOutput(meta, value) {
  if (value == null) {
    return ''
  }
  if (isDateTimeType(meta.dataType)) {
    return String(value).replace('T', ' ')
  }
  return String(value)
}

function prepareModalRowState(mode, sourceFields, columns) {
  const fields = {}
  const nulls = {}
  for (const column of columns) {
    const name = column.name
    const raw = sourceFields?.[name]
    if (isNullLikeValue(raw)) {
      fields[name] = ''
      nulls[name] = true
      continue
    }
    fields[name] = normalizeValueForType(raw ?? '', column.dataType)
    if (mode === 'insert' && column.nullable) {
      nulls[name] = false
    }
  }
  return { fields, nulls }
}

function modalValidationError(modal) {
  if (modal.mode === 'delete') {
    return ''
  }

  for (const column of modal.columns || []) {
    const name = column.name
    const value = String(modal.fields[name] ?? '')
    const trimmed = value.trim()
    const nullable = !!column.nullable
    const isNull = !!modal.nulls[name]
    const dataType = column.dataType

    if (isNull) {
      if (!nullable) {
        return `${name} cannot be NULL`
      }
      continue
    }

    if (isIntegerType(dataType) && trimmed && !/^[-+]?\d+$/.test(trimmed)) {
      return `${name} expects an integer value`
    }

    if (isNumericType(dataType) && trimmed && Number.isNaN(Number(trimmed))) {
      return `${name} expects a numeric value`
    }

    if ((isDateType(dataType) || isDateTimeType(dataType) || isTimeType(dataType)) && trimmed) {
      const valid = isDateType(dataType)
        ? /^\d{4}-\d{2}-\d{2}$/.test(trimmed)
        : isDateTimeType(dataType)
          ? /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(trimmed) || /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(trimmed)
          : /^\d{2}:\d{2}(:\d{2})?$/.test(trimmed)
      if (!valid) {
        return `${name} has an invalid date/time format`
      }
    }

    if (modal.mode === 'insert' && !trimmed && !nullable && !column.defaultValue) {
      return `${name} is required`
    }
  }
  return ''
}

function buildModalRowPayload(modal) {
  const output = {}
  const columns = modal.columns || []

  for (const column of columns) {
    const name = column.name
    if (!name) {
      continue
    }

    const value = modal.fields[name]
    const hasValue = String(value ?? '').trim() !== ''
    const nullable = !!column.nullable
    const isNull = !!modal.nulls[name]

    if (isNull) {
      output[name] = NULL_SENTINEL
      continue
    }

    if (modal.mode === 'insert' && !hasValue) {
      if (nullable || column.defaultValue) {
        continue
      }
    }

    if (!hasValue) {
      output[name] = ''
      continue
    }

    output[name] = normalizeModalFieldOutput(column, value)
  }

  return output
}

function randomUUID() {
  if (globalThis.crypto && typeof globalThis.crypto.randomUUID === 'function') {
    return globalThis.crypto.randomUUID()
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = Math.floor(Math.random() * 16)
    const v = c === 'x' ? r : ((r & 0x3) | 0x8)
    return v.toString(16)
  })
}

function toConnectionPayload(form) {
  const mode = form.mode === 'ssh' ? 'ssh' : 'direct'
  return {
    id: form.id,
    name: form.name,
    type: form.type,
    useConnString: !!form.useConnString,
    connString: form.connString,
    host: form.host,
    port: Number(form.port || 0),
    database: form.database,
    schema: form.schema,
    username: form.username,
    password: form.password,
    ssh: {
      enabled: mode === 'ssh',
      host: form.sshHost,
      port: Number(form.sshPort || 0),
      user: form.sshUser,
      authType: form.sshAuthType,
      password: form.sshPassword,
      keyFile: form.sshKeyFile,
      passphrase: form.sshPassphrase,
    },
  }
}

function fromConnectionRecord(conn) {
  const form = defaultConnectionForm()
  form.id = conn.id || ''
  form.name = conn.name || ''
  form.type = String(conn.type || 'mysql').toLowerCase()
  form.useConnString = !!conn.useConnString
  form.connString = conn.connString || ''
  form.host = conn.host || '127.0.0.1'
  form.port = String(conn.port || defaultPortByType(form.type))
  form.database = conn.database || ''
  form.schema = conn.schema || ''
  form.username = conn.username || ''
  form.password = conn.password || ''
  form.mode = conn.ssh?.enabled ? 'ssh' : 'direct'
  form.sshHost = conn.ssh?.host || ''
  form.sshPort = String(conn.ssh?.port || 22)
  form.sshUser = conn.ssh?.user || ''
  form.sshAuthType = conn.ssh?.authType || 'password'
  form.sshPassword = conn.ssh?.password || ''
  form.sshKeyFile = conn.ssh?.keyFile || ''
  form.sshPassphrase = conn.ssh?.passphrase || ''
  return form
}

function defaultPortByType(type) {
  const key = String(type || '').toLowerCase()
  if (key.includes('post')) {
    return 5432
  }
  if (key.includes('mysql')) {
    return 3306
  }
  if (key.includes('mongo')) {
    return 27017
  }
  if (key.includes('redis')) {
    return 6379
  }
  return 0
}

function normalizeObjects(objects, extraSchemas) {
  const bySchema = {}

  const ensureSchema = (name) => {
    if (!name) {
      return
    }
    if (!bySchema[name]) {
      bySchema[name] = {
        tables: [],
        views: [],
        procedures: [],
        triggers: [],
      }
    }
  }

  for (const schema of extraSchemas || []) {
    ensureSchema(String(schema || '').trim())
  }

  for (const obj of objects || []) {
    const objType = String(obj.type || '').toLowerCase()
    const objSchema = String(obj.schema || '').trim() || String(obj.name || '').trim()
    ensureSchema(objSchema)

    if (objType === 'schema' || objType === 'database') {
      continue
    }

    const name = String(obj.name || '').trim()
    if (!name) {
      continue
    }

    if (objType === 'table' || objType === 'collection') {
      bySchema[objSchema].tables.push(name)
      continue
    }
    if (objType === 'view') {
      bySchema[objSchema].views.push(name)
      continue
    }
    if (objType === 'procedure' || objType === 'function') {
      bySchema[objSchema].procedures.push(name)
      continue
    }
    if (objType === 'trigger') {
      bySchema[objSchema].triggers.push(name)
      continue
    }

    // Redis key types and unknown types are listed under tables.
    bySchema[objSchema].tables.push(name)
  }

  const schemas = Object.keys(bySchema).sort((a, b) => a.localeCompare(b))
  for (const schema of schemas) {
    for (const key of ['tables', 'views', 'procedures', 'triggers']) {
      bySchema[schema][key] = [...new Set(bySchema[schema][key])].sort((a, b) => a.localeCompare(b))
    }
  }

  return { schemas, bySchema }
}

async function loadConnections() {
  state.loadingConnections = true
  render()

  try {
    if (!hasBackend()) {
      const search = state.connectionSearch.trim().toLowerCase()
      const filtered = demoConnections.filter((row) => {
        if (!search) {
          return true
        }
        const bag = `${row.name} ${row.host} ${row.type} ${row.mode} ${row.subline} ${row.port}`.toLowerCase()
        return bag.includes(search)
      })
      state.connectionTotal = filtered.length
      state.connectionTotalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE))
      if (state.connectionPage > state.connectionTotalPages) {
        state.connectionPage = state.connectionTotalPages
      }
      const start = (state.connectionPage - 1) * PAGE_SIZE
      state.connections = filtered.slice(start, start + PAGE_SIZE)
      return
    }

    const page = await callApi('ListConnections', state.connectionSearch, state.connectionPage, PAGE_SIZE)
    state.connections = page.items || []
    state.connectionTotal = Number(page.total || 0)
    state.connectionTotalPages = Math.max(1, Number(page.totalPages || 1))
    state.connectionPage = Math.max(1, Number(page.page || 1))
  } catch (err) {
    notify(String(err?.message || err), true)
  } finally {
    state.loadingConnections = false
    render()
  }
}

async function loadActiveConnection() {
  if (!hasBackend()) {
    state.activeConnection = demoConnections[0]
    return
  }

  const info = await callApi('GetActiveConnection')
  state.activeConnection = info?.hasActive ? info.connection : null
}

async function loadObjectTree() {
  state.loadingObjects = true
  render()
  try {
    if (!hasBackend()) {
      const demoObjects = [
        { schema: 'public', name: 'public', type: 'schema' },
        { schema: 'public', name: 'users', type: 'table' },
        { schema: 'public', name: 'orders', type: 'table' },
        { schema: 'public', name: 'products', type: 'table' },
        { schema: 'public', name: 'inventory', type: 'table' },
        { schema: 'analytics', name: 'events', type: 'table' },
        { schema: 'analytics', name: 'daily_totals', type: 'view' },
      ]
      state.objectTree = normalizeObjects(demoObjects, ['public', 'analytics'])
      if (!state.selectedSchema || !state.objectTree.schemas.includes(state.selectedSchema)) {
        state.selectedSchema = state.objectTree.schemas[0] || ''
      }
      return
    }

    const [objects, schemas] = await Promise.all([
      callApi('ListObjectsActive'),
      callApi('ListSchemasActive'),
    ])

    state.objectTree = normalizeObjects(objects || [], schemas || [])

    if (state.selectedSchema && state.objectTree.schemas.includes(state.selectedSchema)) {
      return
    }

    const fromConnection = String(state.activeConnection?.subline || '').split('•')[0].trim()
    if (fromConnection && state.objectTree.schemas.includes(fromConnection)) {
      state.selectedSchema = fromConnection
      return
    }

    state.selectedSchema = state.objectTree.schemas[0] || ''
  } finally {
    state.loadingObjects = false
    render()
  }
}

async function loadTableOverview() {
  state.loadingTableOverview = true
  state.tableOverviewError = ''
  render()

  try {
    if (!state.selectedSchema) {
      state.tableOverview = []
      return
    }

    if (!hasBackend()) {
      state.tableOverview = [
        { schema: state.selectedSchema, name: 'users', rows: 154203, size: '42.5 MB', last_updated: '2 mins ago' },
        { schema: state.selectedSchema, name: 'orders', rows: 892110, size: '210.8 MB', last_updated: '15 mins ago' },
        { schema: state.selectedSchema, name: 'products', rows: 12400, size: '8.2 MB', last_updated: 'Yesterday' },
        { schema: state.selectedSchema, name: 'inventory', rows: 54000, size: '15.4 MB', last_updated: '2 hours ago' },
      ]
      return
    }

    state.tableOverview = await callApi('ListTableInfoActive', state.selectedSchema)
  } catch (err) {
    state.tableOverview = []
    state.tableOverviewError = String(err?.message || err)
    notify(state.tableOverviewError, true)
  } finally {
    state.loadingTableOverview = false
    render()
  }
}

function triggerFileDownload(filename, content, mimeType = 'text/plain;charset=utf-8') {
  const blob = new Blob([content], { type: mimeType })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}

function csvCell(value) {
  const raw = String(value ?? '')
  if (raw.includes('"') || raw.includes(',') || raw.includes('\n') || raw.includes('\r')) {
    return `"${raw.replaceAll('"', '""')}"`
  }
  return raw
}

function queryResultToCSV(result) {
  const lines = []
  if (result.columns?.length) {
    lines.push(result.columns.map(csvCell).join(','))
  }
  for (const row of result.rows || []) {
    lines.push((row || []).map(csvCell).join(','))
  }
  return lines.join('\n') + '\n'
}

async function exportQueryResults(format = 'csv') {
  const result = state.queryResult || { columns: [], rows: [] }
  if (!result.columns.length && !result.rows.length) {
    notify('Run a query first', true)
    return
  }

  const ext = format === 'json' ? 'json' : 'csv'
  const content = format === 'json'
    ? JSON.stringify({
      columns: result.columns || [],
      rows: result.rows || [],
      rowsAffected: result.rowsAffected || 0,
      message: result.message || '',
      durationMs: result.durationMs || 0,
    }, null, 2)
    : queryResultToCSV(result)

  const type = format === 'json' ? 'application/json;charset=utf-8' : 'text/csv;charset=utf-8'
  const nameParts = [
    safeFilePart(state.activeConnection?.name),
    safeFilePart(state.selectedSchema),
    'query_result',
    new Date().toISOString().replaceAll(':', '').replaceAll('-', '').slice(0, 15),
  ].filter(Boolean)
  const filename = `${nameParts.join('_') || 'query_result'}.${ext}`

  try {
    if (!hasBackend()) {
      triggerFileDownload(filename, content, type)
      notify(`Exported ${ext.toUpperCase()}`)
      return
    }
    const path = await callApi('SaveTextFile', filename, content)
    if (path) {
      notify(`Saved ${ext.toUpperCase()}: ${path}`)
    }
  } catch (err) {
    notify(String(err?.message || err), true)
  }
}

async function openStructureModal(schema, table) {
  const cleanSchema = String(schema || '').trim()
  const cleanTable = String(table || '').trim()
  if (!cleanTable) {
    return
  }

  state.structureModal = {
    open: true,
    loading: true,
    schema: cleanSchema,
    table: cleanTable,
    columns: [],
    error: '',
  }
  render()

  try {
    if (!hasBackend()) {
      const tableState = state.tableStateByTab[tableTabId(cleanSchema, cleanTable)]
      const fallback = normalizeColumnMeta(tableState?.columns || ['id', 'name', 'created_at'], [])
      state.structureModal.columns = fallback
      return
    }

    const columns = await callApi('DescribeTableColumnsActive', cleanSchema, cleanTable)
    state.structureModal.columns = normalizeColumnMeta(
      (columns || []).map((item) => item?.name).filter(Boolean),
      columns || [],
    )
  } catch (err) {
    state.structureModal.error = String(err?.message || err)
  } finally {
    state.structureModal.loading = false
    render()
  }
}

function closeStructureModal() {
  state.structureModal.open = false
  state.structureModal.loading = false
  state.structureModal.error = ''
  render()
}

async function emptyTableFromOverview(schema, table) {
  const cleanSchema = String(schema || '').trim()
  const cleanTable = String(table || '').trim()
  if (!cleanTable) {
    return
  }
  if (!window.confirm(`Empty all rows from ${cleanSchema || '-'}.${cleanTable}?`)) {
    return
  }

  const busyKey = tableActionKey('empty', cleanSchema, cleanTable)
  state.tableActionBusy = busyKey
  render()
  try {
    if (!hasBackend()) {
      notify('Empty table is not available in browser preview mode', true)
      return
    }
    await callApi('EmptyTableActive', cleanSchema, cleanTable)
    for (const tab of state.tableTabs) {
      if (tab.schema === cleanSchema && tab.table === cleanTable) {
        await loadTableData(tab.id)
      }
    }
    await loadTableOverview()
    notify(`Table ${cleanTable} emptied`)
  } catch (err) {
    notify(String(err?.message || err), true)
  } finally {
    state.tableActionBusy = ''
    render()
  }
}

async function dropTableFromOverview(schema, table) {
  const cleanSchema = String(schema || '').trim()
  const cleanTable = String(table || '').trim()
  if (!cleanTable) {
    return
  }
  if (!window.confirm(`Drop table ${cleanSchema || '-'}.${cleanTable}? This cannot be undone.`)) {
    return
  }

  const busyKey = tableActionKey('drop', cleanSchema, cleanTable)
  state.tableActionBusy = busyKey
  render()
  try {
    if (!hasBackend()) {
      notify('Drop table is not available in browser preview mode', true)
      return
    }
    await callApi('DropTableActive', cleanSchema, cleanTable)

    const id = tableTabId(cleanSchema, cleanTable)
    if (state.tableTabs.some((item) => item.id === id)) {
      closeTableTab(id)
    }

    await Promise.all([loadObjectTree(), loadTableOverview()])
    notify(`Table ${cleanTable} dropped`)
  } catch (err) {
    notify(String(err?.message || err), true)
  } finally {
    state.tableActionBusy = ''
    render()
  }
}

async function createTableFromOverview() {
  const schema = String(state.selectedSchema || '').trim()
  if (!schema) {
    notify('Select a schema/database first', true)
    return
  }

  const table = String(window.prompt(`New table name in ${schema}:`, '') || '').trim()
  if (!table) {
    return
  }

  const busyKey = tableActionKey('create', schema, table)
  state.tableActionBusy = busyKey
  render()
  try {
    if (!hasBackend()) {
      notify('Create table is not available in browser preview mode', true)
      return
    }
    await callApi('CreateTableActive', schema, table)
    await Promise.all([loadObjectTree(), loadTableOverview()])
    notify(`Table ${table} created`)
  } catch (err) {
    notify(String(err?.message || err), true)
  } finally {
    state.tableActionBusy = ''
    render()
  }
}

async function exportDatabaseFromOverview() {
  const schema = String(state.selectedSchema || '').trim()
  if (!schema) {
    notify('Select a schema/database first', true)
    return
  }

  const busyKey = tableActionKey('export', schema, '')
  state.tableActionBusy = busyKey
  render()
  try {
    if (!hasBackend()) {
      const sample = `-- Demo export\n-- Schema: ${schema}\n-- Generated: ${new Date().toISOString()}\n`
      triggerFileDownload(`demo_export_${schema}.sql`, sample, 'text/sql;charset=utf-8')
      notify('Demo export downloaded')
      return
    }

    const path = await callApi('ExportDatabaseActive', schema)
    if (path) {
      notify(`Database exported: ${path}`)
    }
  } catch (err) {
    notify(String(err?.message || err), true)
  } finally {
    state.tableActionBusy = ''
    render()
  }
}

async function hydrateWorkspace() {
  try {
    await loadActiveConnection()
    if (!state.activeConnection) {
      state.route = 'connections'
      render()
      return
    }

    await loadObjectTree()
    await loadTableOverview()
    await Promise.all([loadSnippets(), loadHistory()])

    if (state.tableTabs.length > 0 && !state.tableStateByTab[state.activeTableTab]) {
      state.tableTabs = []
      state.activeTableTab = ''
      clearCellInteractions()
    }

    render()
  } catch (err) {
    notify(String(err?.message || err), true)
    state.route = 'connections'
    render()
  }
}

function openNewConnectionDrawer() {
  state.route = 'manager'
  state.manager.title = 'Add New Connection'
  state.manager.selectedID = ''
  state.manager.form = defaultConnectionForm()
  state.manager.busy = false
  state.manager.unsaved = false
  render()
}

async function openEditConnectionDrawer(id) {
  try {
    if (!hasBackend()) {
      const item = state.connections.find((conn) => conn.id === id)
      if (!item) {
        return
      }
      state.route = 'manager'
      state.manager.title = 'Edit Connection'
      state.manager.selectedID = id
      state.manager.form = {
        ...defaultConnectionForm(),
        id,
        name: item.name || '',
        type: String(item.type || 'mysql').toLowerCase(),
        host: item.host || '127.0.0.1',
        port: String(item.port || defaultPortByType(item.type)),
      }
      state.manager.unsaved = false
      render()
      return
    }
    const conn = await callApi('GetConnection', id)
    state.route = 'manager'
    state.manager.title = 'Edit Connection'
    state.manager.selectedID = id
    state.manager.form = fromConnectionRecord(conn)
    state.manager.busy = false
    state.manager.unsaved = false
    render()
  } catch (err) {
    notify(String(err?.message || err), true)
  }
}

async function saveDrawerConnection() {
  const payload = toConnectionPayload(state.manager.form)
  state.manager.busy = true
  render()

  try {
    if (!hasBackend()) {
      notify('Save is not available in browser preview mode', true)
      return
    }

    const saved = await callApi('SaveConnection', payload)
    state.manager.selectedID = saved?.id || payload.id || ''
    state.manager.form.id = state.manager.selectedID
    state.manager.unsaved = false
    notify('Connection saved')
    await loadConnections()
  } catch (err) {
    notify(String(err?.message || err), true)
  } finally {
    state.manager.busy = false
    render()
  }
}

async function testDrawerConnection() {
  state.manager.busy = true
  render()

  try {
    if (!hasBackend()) {
      notify('Test is not available in browser preview mode', true)
      return
    }
    await callApi('TestConnection', toConnectionPayload(state.manager.form))
    notify(state.manager.form.mode === 'ssh' ? 'SSH tunnel test successful' : 'Connection test successful')
  } catch (err) {
    notify(String(err?.message || err), true)
  } finally {
    state.manager.busy = false
    render()
  }
}

async function pickManagerSSHKeyFile() {
  try {
    if (!hasBackend()) {
      notify('File picker is available only in desktop app', true)
      return
    }
    const path = await callApi('PickSSHKeyFile')
    if (!path) {
      return
    }
    state.manager.form.sshKeyFile = String(path)
    state.manager.unsaved = true
    render()
  } catch (err) {
    notify(String(err?.message || err), true)
  }
}

async function deleteConnection(id) {
  if (!window.confirm('Delete this connection?')) {
    return
  }

  try {
    if (!hasBackend()) {
      notify('Delete is not available in browser preview mode', true)
      return
    }

    await callApi('DeleteConnection', id)
    notify('Connection deleted')
    await loadConnections()
  } catch (err) {
    notify(String(err?.message || err), true)
  }
}

async function deleteManagerConnection() {
  const id = state.manager.form.id || state.manager.selectedID
  if (!id) {
    notify('Select a saved connection first', true)
    return
  }
  await deleteConnection(id)
  state.manager.selectedID = ''
  state.manager.title = 'Add New Connection'
  state.manager.form = defaultConnectionForm()
  state.manager.unsaved = false
  render()
}

async function selectConnectionInManager(id) {
  if (!id) {
    return
  }
  await openEditConnectionDrawer(id)
}

async function openConnection(id) {
  if (!id) {
    return
  }
  if (state.openingConnectionID) {
    return
  }
  state.openingConnectionID = id
  render()

  try {
    if (hasBackend()) {
      await callApi('OpenConnection', id)
    }
    state.route = 'workspace'
    state.workspaceTab = 'explorer'
    state.tableTabs = []
    state.activeTableTab = ''
    state.tableStateByTab = {}
    state.tableListSearch = ''
    clearCellInteractions()
    await hydrateWorkspace()
    notify('Connection opened')
  } catch (err) {
    notify(String(err?.message || err), true)
  } finally {
    state.openingConnectionID = ''
    render()
  }
}

async function disconnectActive() {
  try {
    if (!hasBackend()) {
      state.route = 'connections'
      state.activeConnection = null
      render()
      return
    }

    await callApi('DisconnectActive')
    state.activeConnection = null
    state.route = 'connections'
    state.workspaceTab = 'explorer'
    state.tableTabs = []
    state.activeTableTab = ''
    state.tableStateByTab = {}
    state.tableListSearch = ''
    clearCellInteractions()
    await loadConnections()
    notify('Disconnected')
  } catch (err) {
    notify(String(err?.message || err), true)
  }
}

function buildSortClause(column, direction) {
  if (!column) {
    return ''
  }

  const dir = direction === 'DESC' ? 'DESC' : 'ASC'
  const type = activeType()

  if (type === 'mongo') {
    const val = dir === 'ASC' ? 1 : -1
    return JSON.stringify({ [column]: val })
  }

  if (type === 'redis') {
    return ''
  }

  if (type === 'mysql') {
    return `\`${String(column).replaceAll('`', '')}\` ${dir}`
  }

  return `"${String(column).replaceAll('"', '')}" ${dir}`
}

function ensureTableState(tabId) {
  if (!state.tableStateByTab[tabId]) {
    const tab = state.tableTabs.find((item) => item.id === tabId)
    if (!tab) {
      return null
    }
    state.tableStateByTab[tabId] = blankTableState(tab.schema, tab.table)
  }
  return state.tableStateByTab[tabId]
}

function openCellViewerAt(rowIndex, colIndex) {
  const tableState = currentTableState()
  if (!tableState) {
    return
  }

  const row = tableState.rows[rowIndex]
  if (!row || colIndex < 0 || colIndex >= tableState.columns.length) {
    return
  }

  const rawValue = cellTextValue(row[colIndex])
  const decoded = decodeJSONValue(rawValue)
  state.cellViewer = {
    open: true,
    tabId: state.activeTableTab,
    rowIndex,
    colIndex,
    value: rawValue,
    prettyValue: decoded.pretty,
    isJSON: decoded.isJSON,
  }
  clearInlineCell()
  render()
}

async function openTableTab(schema, table) {
  const id = tableTabId(schema, table)
  if (!state.tableTabs.some((item) => item.id === id)) {
    state.tableTabs.push({ id, schema, table })
  }
  state.activeTableTab = id
  ensureTableState(id)
  state.workspaceTab = 'table'
  render()
  await loadTableData(id)
}

function closeTableTab(id) {
  state.tableTabs = state.tableTabs.filter((tab) => tab.id !== id)
  delete state.tableStateByTab[id]
  clearCellInteractions(id)

  if (state.activeTableTab === id) {
    state.activeTableTab = state.tableTabs.length ? state.tableTabs[state.tableTabs.length - 1].id : ''
  }

  if (!state.activeTableTab) {
    state.workspaceTab = 'explorer'
  }

  render()
}

async function loadTableData(tabId = state.activeTableTab) {
  const tab = state.tableTabs.find((item) => item.id === tabId)
  if (!tab) {
    return
  }

  clearCellInteractions(tabId)
  const tableState = ensureTableState(tabId)
  tableState.loading = true
  tableState.error = ''
  render()

  try {
    if (!hasBackend()) {
      const rows = []
      for (let i = 0; i < tableState.limit; i += 1) {
        const id = (tableState.page - 1) * tableState.limit + i + 1
        rows.push([
          String(id),
          `user_${id}@example.com`,
          String((id * 13) % 900 + 100),
          id % 2 === 0 ? 'active' : 'pending',
          `2026-02-25 10:${String(id % 60).padStart(2, '0')}:00`,
        ])
      }
      tableState.columns = ['id', 'email', 'amount', 'status', 'created_at']
      tableState.columnWidths = ensureColumnWidths(tableState.columns, tableState.columnWidths)
      tableState.columnMeta = normalizeColumnMeta(tableState.columns, tableState.columnMeta)
      tableState.rows = rows
      tableState.totalRows = 12450
      tableState.from = (tableState.page - 1) * tableState.limit + 1
      tableState.to = tableState.from + rows.length - 1
      tableState.hasNext = tableState.to < tableState.totalRows
      return
    }

    const payload = {
      schema: tab.schema,
      table: tab.table,
      filter: tableState.filter,
      sort: tableState.sort,
      limit: Number(tableState.limit || 50),
      page: Number(tableState.page || 1),
    }

    const [data, totalRows] = await Promise.all([
      callApi('FetchTableDataActive', payload),
      callApi('CountTableRowsActive', tab.schema, tab.table, tableState.filter),
    ])

    tableState.columns = data.columns || []
    tableState.columnWidths = ensureColumnWidths(tableState.columns, tableState.columnWidths)
    tableState.columnMeta = normalizeColumnMeta(tableState.columns, tableState.columnMeta)
    tableState.rows = data.rows || []
    tableState.from = Number(data.from || 0)
    tableState.to = Number(data.to || 0)
    tableState.hasNext = !!data.hasNext
    tableState.totalRows = Number(totalRows || 0)
    tableState.selectedRow = -1
  } catch (err) {
    tableState.error = String(err?.message || err)
    notify(tableState.error, true)
  } finally {
    tableState.loading = false
    render()
  }
}

function setTableSort(column) {
  const tableState = currentTableState()
  if (!tableState) {
    return
  }

  if (tableState.sortColumn === column) {
    tableState.sortDirection = tableState.sortDirection === 'ASC' ? 'DESC' : 'ASC'
  } else {
    tableState.sortColumn = column
    tableState.sortDirection = 'ASC'
  }

  tableState.sort = buildSortClause(tableState.sortColumn, tableState.sortDirection)
  tableState.sortDraft = tableState.sort
  tableState.page = 1
  loadTableData(state.activeTableTab)
}

function selectedRowMap(tabId) {
  const tableState = state.tableStateByTab[tabId]
  if (!tableState || tableState.selectedRow < 0 || tableState.selectedRow >= tableState.rows.length) {
    return null
  }

  const row = tableState.rows[tableState.selectedRow]
  const data = {}
  for (let i = 0; i < tableState.columns.length; i += 1) {
    data[tableState.columns[i]] = row[i] ?? ''
  }
  return data
}

async function openRowModal(mode) {
  const tabId = state.activeTableTab
  const tab = state.tableTabs.find((item) => item.id === tabId)
  const tableState = currentTableState()

  if (!tab || !tableState) {
    notify('Open a table first', true)
    return
  }

  if (mode !== 'insert' && tableState.selectedRow < 0) {
    notify('Select a row first', true)
    return
  }

  const fallbackColumns = rowModalColumns(tabId, tableState.columns)
  const sourceFields = mode === 'insert' ? {} : selectedRowMap(tabId)
  if (mode !== 'insert' && !sourceFields) {
    notify('Select a row first', true)
    return
  }

  const prepared = prepareModalRowState(mode, sourceFields, fallbackColumns)
  let keyColumn = fallbackColumns.find((column) => column.isPrimary)?.name || fallbackColumns[0]?.name || ''
  let keyValue = mode === 'insert' ? '' : String(sourceFields?.[keyColumn] ?? '')
  if (isNullLikeValue(keyValue)) {
    keyValue = ''
  }

  state.rowModal = {
    open: true,
    busy: false,
    loading: true,
    mode,
    tabId,
    fields: prepared.fields,
    nulls: prepared.nulls,
    columns: fallbackColumns,
    keyColumn,
    keyValue,
  }

  render()

  const metaColumns = await loadTableColumnMeta(tabId)
  if (!state.rowModal.open || state.rowModal.tabId !== tabId || state.rowModal.mode !== mode) {
    return
  }

  const finalColumns = metaColumns.length ? metaColumns : fallbackColumns
  const finalPrepared = prepareModalRowState(mode, sourceFields, finalColumns)
  state.rowModal.columns = finalColumns
  state.rowModal.fields = finalPrepared.fields
  state.rowModal.nulls = finalPrepared.nulls

  if (!finalColumns.some((column) => column.name === state.rowModal.keyColumn)) {
    state.rowModal.keyColumn = finalColumns.find((column) => column.isPrimary)?.name || finalColumns[0]?.name || ''
  }
  if (mode !== 'insert') {
    const currentKeyRaw = sourceFields?.[state.rowModal.keyColumn]
    state.rowModal.keyValue = isNullLikeValue(currentKeyRaw) ? '' : String(currentKeyRaw ?? '')
  }

  state.rowModal.loading = false
  render()
}

async function submitRowModal() {
  const modal = state.rowModal
  const tab = state.tableTabs.find((item) => item.id === modal.tabId)
  if (!modal.open || !tab) {
    return
  }

  if (modal.loading) {
    return
  }

  if ((modal.mode === 'update' || modal.mode === 'delete') && !String(modal.keyColumn || '').trim()) {
    notify('Key column is required', true)
    return
  }
  if ((modal.mode === 'update' || modal.mode === 'delete') && !String(modal.keyValue || '').trim()) {
    notify('Key value is required', true)
    return
  }

  const validationError = modalValidationError(modal)
  if (validationError) {
    notify(validationError, true)
    return
  }

  const payloadRow = buildModalRowPayload(modal)
  if (modal.mode === 'insert' && Object.keys(payloadRow).length === 0) {
    notify('Provide at least one field value', true)
    return
  }

  modal.busy = true
  render()

  try {
    if (!hasBackend()) {
      notify('Row actions are not available in browser preview mode', true)
      return
    }

    if (modal.mode === 'insert') {
      await callApi('InsertRowActive', {
        schema: tab.schema,
        table: tab.table,
        row: payloadRow,
      })
      notify('Row inserted')
    } else if (modal.mode === 'update') {
      await callApi('UpdateRowActive', {
        schema: tab.schema,
        table: tab.table,
        keyColumn: modal.keyColumn,
        keyValue: modal.keyValue,
        row: payloadRow,
      })
      notify('Row updated')
    } else {
      await callApi('DeleteRowActive', {
        schema: tab.schema,
        table: tab.table,
        keyColumn: modal.keyColumn,
        keyValue: modal.keyValue,
      })
      notify('Row deleted')
    }

    state.rowModal.open = false
    await Promise.all([loadTableData(modal.tabId), loadTableOverview()])
  } catch (err) {
    notify(String(err?.message || err), true)
  } finally {
    modal.busy = false
    render()
  }
}

async function runQueryFromEditor() {
  const editor = root.querySelector('#query-editor')
  const text = editor ? editor.value : state.queryEditor
  let query = text

  if (editor && editor.selectionStart !== editor.selectionEnd) {
    query = text.slice(editor.selectionStart, editor.selectionEnd)
  }

  query = String(query || '').trim()
  if (!query) {
    notify('Query is empty', true)
    return
  }

  state.runningQuery = true
  render()

  try {
    if (!hasBackend()) {
      state.queryResult = {
        columns: ['id', 'email', 'created_at'],
        rows: [
          ['7f2a-4b91-88c2', 'alex@example.com', '2026-02-25 14:22:01'],
          ['9d1x-1c23-09z5', 'jordan@test.io', '2026-02-25 14:15:44'],
          ['4k8l-3h11-12f9', 'corp@bank.com', '2026-02-25 14:02:12'],
        ],
        rowsAffected: 0,
        message: '3 row(s)',
        durationMs: 12,
      }
      state.queryResultColumnWidths = ensureColumnWidths(state.queryResult.columns, state.queryResultColumnWidths)
      notify('Query executed')
      return
    }

    const result = await callApi('RunQueryActive', query)
    state.queryResult = {
      columns: result.columns || [],
      rows: result.rows || [],
      rowsAffected: Number(result.rowsAffected || 0),
      message: result.message || '',
      durationMs: Number(result.durationMs || 0),
    }
    state.queryResultColumnWidths = ensureColumnWidths(state.queryResult.columns, state.queryResultColumnWidths)
    await loadHistory()
    notify('Query executed')
  } catch (err) {
    notify(String(err?.message || err), true)
  } finally {
    state.runningQuery = false
    render()
  }
}

function formatQueryInEditor() {
  const editor = root.querySelector('#query-editor')
  if (!editor) {
    return
  }

  const lines = editor.value
    .split('\n')
    .map((line) => line.trimEnd())
  editor.value = `${lines.join('\n').trim()}\n`
  state.queryEditor = editor.value
}

async function saveSnippetFromEditor() {
  const editor = root.querySelector('#query-editor')
  const query = String(editor ? editor.value : state.queryEditor).trim()
  if (!query) {
    notify('Query is empty', true)
    return
  }

  const name = window.prompt('Snippet name', `Snippet ${new Date().toLocaleString()}`)
  if (name === null) {
    return
  }

  try {
    if (!hasBackend()) {
      notify('Snippets are not available in browser preview mode', true)
      return
    }

    await callApi('SaveSnippetActive', name, query)
    await loadSnippets()
    notify('Snippet saved')
  } catch (err) {
    notify(String(err?.message || err), true)
  }
}

async function loadSnippets() {
  if (!hasBackend()) {
    state.snippets = [
      {
        id: 'demo-snippet',
        name: 'Top Orders',
        query: 'SELECT * FROM orders ORDER BY created_at DESC LIMIT 100;',
        updated_at: new Date().toISOString(),
      },
    ]
    return
  }

  try {
    state.snippets = await callApi('ListSnippets', true)
  } catch (err) {
    notify(String(err?.message || err), true)
  }
}

async function deleteSnippet(id) {
  try {
    if (!hasBackend()) {
      notify('Delete snippet is not available in browser preview mode', true)
      return
    }
    await callApi('DeleteSnippet', id)
    await loadSnippets()
    render()
  } catch (err) {
    notify(String(err?.message || err), true)
  }
}

async function loadHistory() {
  if (!hasBackend()) {
    state.history = [
      {
        id: 'demo-history',
        query: 'SELECT * FROM users LIMIT 100',
        duration_ms: 12,
        executed_at: new Date().toISOString(),
      },
    ]
    return
  }

  try {
    state.history = await callApi('ListHistory', true)
  } catch (err) {
    notify(String(err?.message || err), true)
  }
}

function applyThemeClass() {
  document.body.classList.toggle('theme-light', state.theme === 'light')
}

function renderTopBar() {
  const workspace = state.route === 'workspace'
  const searchPlaceholder = workspace ? 'Search tables, views or queries...' : 'Search connections...'
  const searchValue = state.route === 'manager'
    ? state.manager.search
    : state.route === 'workspace'
      ? state.tableListSearch
      : state.connectionSearch

  return `
    <header class="topbar">
      <div class="brand-wrap">
        <div class="brand-icon">DB</div>
        <div class="brand-name">DB Explorer</div>
        ${workspace ? `
          <nav class="top-nav">
            <button class="top-nav-item ${state.workspaceTab === 'explorer' ? 'active' : ''}" data-action="switch-workspace" data-view="explorer">Databases</button>
            <button class="top-nav-item ${state.workspaceTab === 'table' ? 'active' : ''}" data-action="switch-workspace" data-view="table">Table Data</button>
            <button class="top-nav-item ${state.workspaceTab === 'query' ? 'active' : ''}" data-action="switch-workspace" data-view="query">Query Studio</button>
          </nav>
        ` : ''}
      </div>

      <div class="header-search">
        <span class="search-icon">Search</span>
        <input id="global-search" type="text" placeholder="${searchPlaceholder}" value="${esc(searchValue)}" />
      </div>

      <div class="top-actions">
        <select id="theme-select" class="theme-select" aria-label="Theme">
          <option value="dark" ${state.theme === 'dark' ? 'selected' : ''}>Dark</option>
          <option value="light" ${state.theme === 'light' ? 'selected' : ''}>Light</option>
        </select>
        <button class="btn btn-primary" data-action="new-connection">+ New Connection</button>
      </div>
    </header>
  `
}

function renderConnectionsSidebar() {
  return `
    <aside class="left-sidebar">
      <div class="sidebar-group">
        <div class="sidebar-title">NAVIGATION</div>
        <button class="side-link">Favorites</button>
        <button class="side-link active">Recent</button>
        <button class="side-link">By Project</button>
      </div>

      <div class="sidebar-group">
        <div class="sidebar-title">CLUSTERS</div>
        <button class="side-link">AWS Production</button>
        <button class="side-link">Local Dev</button>
      </div>
    </aside>
  `
}

function renderConnectionRows() {
  if (state.loadingConnections) {
    return loadingMarkup('Loading connections...')
  }

  if (!state.connections.length) {
    return '<div class="empty-row">No connections found. Use New Connection to add one.</div>'
  }

  return state.connections
    .map((row) => {
      const statusClass = String(row.status || '').toLowerCase() === 'live' ? 'live' : 'inactive'
      const modeClass = String(row.mode || '').toLowerCase().includes('ssh') ? 'ssh' : 'direct'
      const opening = state.openingConnectionID === row.id
      return `
        <div class="conn-row ${opening ? 'opening' : ''}" data-action="open-connection" data-id="${esc(row.id)}">
          <div class="conn-name-cell">
            <div class="db-badge ${dbTypeClass(row.type)}">${esc(dbTypeGlyph(row.type))}</div>
            <div>
              <div class="conn-title">${esc(row.name)}</div>
              <div class="conn-subtitle">${esc(dbTypeLabel(row.type))} ${row.subline ? `• ${esc(row.subline)}` : ''}</div>
            </div>
          </div>
          <div class="cell-muted">${esc(row.host)}</div>
          <div class="cell-muted">${esc(row.port)}</div>
          <div><span class="mode-pill ${modeClass}">${esc(row.mode)}</span></div>
          <div><span class="status-pill ${statusClass}">${esc(row.status)}</span></div>
          <div class="row-actions">
            <button class="mini-btn" data-action="open-connection" data-id="${esc(row.id)}" ${state.openingConnectionID ? 'disabled' : ''}>
              ${opening ? '<span class="loading-spinner tiny"></span> Opening...' : 'Open'}
            </button>
            <button class="mini-btn" data-action="edit-connection" data-id="${esc(row.id)}" ${state.openingConnectionID ? 'disabled' : ''}>Edit</button>
            <button class="mini-btn danger" data-action="delete-connection" data-id="${esc(row.id)}" ${state.openingConnectionID ? 'disabled' : ''}>Delete</button>
          </div>
        </div>
      `
    })
    .join('')
}

function renderConnectionsView() {
  const maxPage = Math.max(1, state.connectionTotalPages)
  const center = Math.min(Math.max(1, state.connectionPage), maxPage)
  const pages = []
  for (let i = Math.max(1, center - 1); i <= Math.min(maxPage, center + 1); i += 1) {
    pages.push(i)
  }

  return `
    <div class="connections-screen">
      <main class="page page-connections">
        <div class="crumbs">Dashboard &gt; All Connections</div>
        <h1 class="title">Database Connections</h1>
        <div class="subtitle">Manage and monitor all your active database endpoints</div>

        <div class="table-toolbar">
          <button class="btn btn-icon" data-action="filter-connections" title="Filter">F</button>
          <button class="btn btn-icon" data-action="refresh-connections" title="Refresh">R</button>
        </div>

        <section class="connections-card">
          <div class="connections-head">
            <div>CONNECTION NAME</div>
            <div>HOST ADDRESS</div>
            <div>PORT</div>
            <div>TYPE</div>
            <div>STATUS</div>
            <div>ACTIONS</div>
          </div>
          ${renderConnectionRows()}
        </section>

        <div class="connections-footer">
          <div>Showing ${state.connections.length} of ${state.connectionTotal} connections</div>
          <div class="pager">
            <button class="pager-btn" data-action="connections-prev" ${state.connectionPage <= 1 ? 'disabled' : ''}>Previous</button>
            ${pages.map((p) => `<button class="page-chip ${p === state.connectionPage ? 'active' : 'ghost'}" data-action="connections-page" data-page="${p}">${p}</button>`).join('')}
            <button class="pager-btn" data-action="connections-next" ${state.connectionPage >= state.connectionTotalPages ? 'disabled' : ''}>Next</button>
          </div>
        </div>
      </main>
      ${state.openingConnectionID ? `
        <div class="connection-loading-overlay">
          ${loadingMarkup('Opening connection...')}
        </div>
      ` : ''}
    </div>
  `
}

function managerFilteredConnections() {
  const query = state.manager.search.trim().toLowerCase()
  if (!query) {
    return state.connections
  }
  return state.connections.filter((row) => {
    const bag = `${row.name} ${row.host} ${row.type} ${row.mode} ${row.subline} ${row.port}`.toLowerCase()
    return bag.includes(query)
  })
}

function renderManagerAppNav() {
  return `
    <aside class="manager-app-nav">
      <div class="manager-app-brand">
        <div class="brand-icon">DB</div>
        <div>
          <div class="manager-app-name">DB Manager</div>
          <div class="manager-app-version">Enterprise v2.4.0</div>
        </div>
      </div>

      <div class="manager-app-links">
        <button class="manager-link">Dashboard</button>
        <button class="manager-link active">Connections</button>
        <button class="manager-link">SQL Editor</button>
        <button class="manager-link">Query History</button>
      </div>

      <div class="manager-app-footer">
        <button class="manager-link">Settings</button>
        <button class="manager-link">Support</button>
      </div>
    </aside>
  `
}

function renderManagerConnectionList() {
  const items = managerFilteredConnections()
  if (!items.length) {
    return '<div class="empty-row">No connections found.</div>'
  }
  return items
    .map((row) => {
      const active = row.id === state.manager.selectedID || row.id === state.manager.form.id
      const statusClass = String(row.status || '').toLowerCase() === 'live' ? 'live' : 'inactive'
      return `
        <button class="manager-conn-item ${active ? 'active' : ''}" data-action="manager-select-connection" data-id="${esc(row.id)}">
          <div class="manager-conn-left">
            <div class="db-badge ${dbTypeClass(row.type)}">${esc(dbTypeShort(row.type))}</div>
            <div>
              <div class="manager-conn-title">${esc(row.name)}</div>
              <div class="manager-conn-sub">${esc(row.subline || row.type)}</div>
            </div>
          </div>
          <span class="manager-dot ${statusClass}"></span>
        </button>
      `
    })
    .join('')
}

function renderManagerFormPanel() {
  const f = state.manager.form
  const sshMode = f.mode === 'ssh'
  const showManual = !f.useConnString
  const keyAuth = f.sshAuthType === 'key_file'

  return `
    <section class="manager-form-panel">
      <div class="manager-form-head">
        <div>
          <h2>${esc(state.manager.title)}</h2>
          <div>Configure your database connection parameters</div>
        </div>
        <span class="manager-unsaved ${state.manager.unsaved ? 'show' : ''}">UNSAVED CHANGES</span>
      </div>

      <div class="manager-mode-switch">
        <button class="mode-btn ${!sshMode ? 'active' : ''}" data-action="manager-mode" data-mode="direct">Direct Connection</button>
        <button class="mode-btn ${sshMode ? 'active' : ''}" data-action="manager-mode" data-mode="ssh">SSH Tunnel</button>
      </div>

      <div class="manager-section-title">BASIC SETTINGS</div>
      <div class="manager-form-grid two">
        <label class="field">
          <span>Connection Name</span>
          <input type="text" data-conn-field="name" value="${esc(f.name)}" placeholder="e.g. Finance Production" />
        </label>
        <label class="field">
          <span>Database Type</span>
          <select data-conn-field="type">
            <option value="postgres" ${f.type === 'postgres' ? 'selected' : ''}>PostgreSQL</option>
            <option value="mysql" ${f.type === 'mysql' ? 'selected' : ''}>MySQL</option>
            <option value="mongo" ${f.type === 'mongo' ? 'selected' : ''}>MongoDB</option>
            <option value="redis" ${f.type === 'redis' ? 'selected' : ''}>Redis</option>
          </select>
        </label>
      </div>

      <label class="toggle-field full manager-toggle">
        <input type="checkbox" data-conn-field="useConnString" ${f.useConnString ? 'checked' : ''} />
        <span>Use connection string</span>
      </label>

      ${sshMode ? `
        <div class="manager-section-title">SSH TUNNEL SETTINGS</div>
        <div class="manager-form-grid two">
          <label class="field">
            <span>SSH Host</span>
            <input type="text" data-conn-field="sshHost" value="${esc(f.sshHost)}" placeholder="tunnel.example.com" />
          </label>
          <label class="field">
            <span>Port</span>
            <input type="number" data-conn-field="sshPort" value="${esc(f.sshPort)}" placeholder="22" />
          </label>
          <label class="field">
            <span>SSH User</span>
            <input type="text" data-conn-field="sshUser" value="${esc(f.sshUser)}" placeholder="root" />
          </label>
          <label class="field">
            <span>Auth Method</span>
            <div class="manager-auth-switch">
              <button class="mode-btn ${!keyAuth ? 'active' : ''}" data-action="manager-auth" data-auth="password">Password</button>
              <button class="mode-btn ${keyAuth ? 'active' : ''}" data-action="manager-auth" data-auth="key_file">Private Key</button>
            </div>
          </label>
          ${keyAuth ? `
            <label class="field full">
              <span>Private Key (.pem, .ppk)</span>
              <div class="file-picker-row">
                <input type="text" data-conn-field="sshKeyFile" value="${esc(f.sshKeyFile)}" placeholder="/home/user/.ssh/id_rsa" />
                <button class="btn btn-secondary file-picker-btn" data-action="manager-pick-key">Browse</button>
              </div>
            </label>
            <label class="field full">
              <span>Passphrase</span>
              <input type="password" data-conn-field="sshPassphrase" value="${esc(f.sshPassphrase)}" />
            </label>
          ` : `
            <label class="field full">
              <span>SSH Password</span>
              <input type="password" data-conn-field="sshPassword" value="${esc(f.sshPassword)}" />
            </label>
          `}
        </div>
      ` : ''}

      <div class="manager-section-title">DATABASE ACCESS</div>
      ${showManual ? `
        <div class="manager-form-grid two">
          <label class="field">
            <span>Host</span>
            <input type="text" data-conn-field="host" value="${esc(f.host)}" placeholder="127.0.0.1" />
          </label>
          <label class="field">
            <span>Port</span>
            <input type="number" data-conn-field="port" value="${esc(f.port)}" />
          </label>
          <label class="field">
            <span>Database</span>
            <input type="text" data-conn-field="database" value="${esc(f.database)}" />
          </label>
          <label class="field">
            <span>Schema</span>
            <input type="text" data-conn-field="schema" value="${esc(f.schema)}" placeholder="public" />
          </label>
          <label class="field">
            <span>Username</span>
            <input type="text" data-conn-field="username" value="${esc(f.username)}" />
          </label>
          <label class="field">
            <span>Password</span>
            <input type="password" data-conn-field="password" value="${esc(f.password)}" />
          </label>
        </div>
      ` : `
        <label class="field full">
          <span>Connection String ${sshMode ? '(via tunnel)' : ''}</span>
          <textarea data-conn-field="connString" rows="4" placeholder="postgresql://user:password@host:port/database">${esc(f.connString)}</textarea>
        </label>
      `}

      ${sshMode && !f.useConnString ? `
        <label class="field full">
          <span>Connection String (via tunnel)</span>
          <textarea data-conn-field="connString" rows="3" placeholder="Optional: use full URI via SSH tunnel">${esc(f.connString)}</textarea>
        </label>
      ` : ''}

      <div class="manager-form-actions">
        <button class="btn btn-danger" data-action="manager-delete" ${!state.manager.form.id ? 'disabled' : ''}>Delete</button>
        <div class="manager-form-actions-right">
          <button class="btn btn-secondary" data-action="manager-test" ${state.manager.busy ? 'disabled' : ''}>${sshMode ? 'Test Tunnel' : 'Test Connection'}</button>
          <button class="btn btn-primary" data-action="manager-save" ${state.manager.busy ? 'disabled' : ''}>${state.manager.busy ? 'Saving...' : 'Save Connection'}</button>
        </div>
      </div>
    </section>
  `
}

function renderConnectionManagerView() {
  return `
    <div class="manager-shell">
      ${renderManagerAppNav()}
      <section class="manager-list-panel">
        <div class="manager-list-head">Connections Manager</div>
        <div class="manager-list-items">${renderManagerConnectionList()}</div>
        <div class="manager-list-actions">
          <button class="btn btn-secondary full" data-action="manager-new">+ New Connection</button>
          <button class="btn btn-primary full" data-action="manager-open-selected" ${!state.manager.form.id ? 'disabled' : ''}>Open Connection</button>
          <button class="btn btn-secondary full" data-action="manager-back">Back to Dashboard</button>
        </div>
      </section>
      ${renderManagerFormPanel()}
    </div>
  `
}

function renderObjectSchemaList() {
  if (state.loadingObjects) {
    return loadingMarkup('Loading databases and objects...')
  }
  if (!state.objectTree.schemas.length) {
    return '<div class="empty-small">No schemas found</div>'
  }

  return state.objectTree.schemas
    .map((schema) => {
      const schemaData = state.objectTree.bySchema[schema] || { tables: [], views: [], procedures: [], triggers: [] }
      const isActive = schema === state.selectedSchema
      const visibleTables = schemaData.tables
        .filter((table) => tableListMatch(table, schema))
      const tables = visibleTables
        .slice(0, 120)
        .map((table) => {
          const tabId = tableTabId(schema, table)
          const open = state.tableTabs.some((tab) => tab.id === tabId)
          return `
            <button class="tree-item table ${open ? 'open' : ''}" data-action="open-table" data-schema="${esc(schema)}" data-table="${esc(table)}">
              ${esc(table)}
            </button>
          `
        })
        .join('')

      return `
        <div class="schema-block ${isActive ? 'active' : ''}">
          <button class="schema-btn" data-action="select-schema" data-schema="${esc(schema)}">${esc(schema)}</button>
          ${isActive ? `<div class="schema-table-list">${tables || '<div class="empty-small">No tables match filter</div>'}</div>` : ''}
        </div>
      `
    })
    .join('')
}

function renderWorkspaceSidebar() {
  const active = state.activeConnection
  return `
    <aside class="workspace-sidebar">
      <section class="card active-db-card">
        <div class="section-label">ACTIVE DATABASE</div>
        <div class="active-db-name">${esc(active?.name || '-')}</div>
        <div class="active-db-sub">${esc((active?.mode || 'DIRECT').toUpperCase())}</div>
      </section>

      <section class="card nav-card">
        <div class="section-label">OBJECTS</div>
        <div class="schema-list">${renderObjectSchemaList()}</div>
      </section>

      <section class="card connection-card-side">
        <div class="section-label">CONNECTION</div>
        <div class="connection-name">${esc(active?.name || '-')}</div>
        <div class="connection-meta">${esc(active?.type || '')} | ${esc(active?.mode || '')}</div>
        <div class="connection-actions">
          <button class="mini-btn" data-action="go-connections">Connections</button>
          <button class="mini-btn danger" data-action="disconnect-active">Disconnect</button>
        </div>
      </section>
    </aside>
  `
}

function renderTableOverviewRows() {
  if (state.loadingTableOverview) {
    return loadingMarkup('Loading tables...')
  }
  if (state.tableOverviewError) {
    return `<div class="empty-row">${esc(state.tableOverviewError)}</div>`
  }
  if (!state.tableOverview.length) {
    return '<div class="empty-row">No tables found for this schema/database.</div>'
  }

  const filtered = filteredTableOverviewItems()
  if (!filtered.length) {
    return '<div class="empty-row">No tables match the current table search.</div>'
  }

  return filtered
    .map((table) => {
      const schema = table.schema || state.selectedSchema
      const tableName = table.name
      const isEmptyBusy = state.tableActionBusy === tableActionKey('empty', schema, tableName)
      const isDropBusy = state.tableActionBusy === tableActionKey('drop', schema, tableName)
      return `
        <div class="table-overview-row">
          <button class="link-cell" data-action="table-browse" data-schema="${esc(schema)}" data-table="${esc(tableName)}">${esc(table.name)}</button>
          <div>${esc(table.rows)}</div>
          <div>${esc(table.size)}</div>
          <div>${esc(table.last_updated || table.lastUpdated || 'unknown')}</div>
          <div class="overview-actions">
            <button class="mini-btn" data-action="table-browse" data-schema="${esc(schema)}" data-table="${esc(tableName)}">Browse</button>
            <button class="mini-btn" data-action="table-structure" data-schema="${esc(schema)}" data-table="${esc(tableName)}">Structure</button>
            <button class="mini-btn" data-action="table-empty" data-schema="${esc(schema)}" data-table="${esc(tableName)}" ${isEmptyBusy ? 'disabled' : ''}>${isEmptyBusy ? 'Emptying...' : 'Empty'}</button>
            <button class="mini-btn danger" data-action="table-drop" data-schema="${esc(schema)}" data-table="${esc(tableName)}" ${isDropBusy ? 'disabled' : ''}>${isDropBusy ? 'Dropping...' : 'Drop'}</button>
          </div>
        </div>
      `
    })
    .join('')
}

function renderExplorerView() {
  const visibleTables = filteredTableOverviewItems()
  const totalRows = visibleTables.reduce((sum, item) => sum + Number(item.rows || 0), 0)
  const hasTableSearch = tableListQuery() !== ''

  return `
    <section class="workspace-view">
      <div class="workspace-head">
        <div>
          <h2 class="section-title">Tables Overview</h2>
          <div class="section-subtitle">Showing all tables in schema ${esc(state.selectedSchema || '-')}</div>
        </div>
        <div class="workspace-head-actions">
          <button class="btn btn-secondary compact" data-action="refresh-overview">Refresh</button>
          <button class="btn btn-secondary compact" data-action="export-database" ${state.tableActionBusy === tableActionKey('export', state.selectedSchema, '') ? 'disabled' : ''}>${state.tableActionBusy === tableActionKey('export', state.selectedSchema, '') ? 'Exporting...' : 'Export Database'}</button>
          <button class="btn btn-secondary compact" data-action="create-table" ${state.tableActionBusy.startsWith('create:') ? 'disabled' : ''}>${state.tableActionBusy.startsWith('create:') ? 'Creating...' : 'Create New Table'}</button>
          <button class="btn btn-primary compact" data-action="switch-workspace" data-view="query">Open Query Editor</button>
        </div>
      </div>

      <div class="table-overview">
        <div class="table-overview-head">
          <div>TABLE NAME</div>
          <div>ROW COUNT</div>
          <div>SIZE</div>
          <div>LAST UPDATED</div>
          <div>ACTION</div>
        </div>
        ${renderTableOverviewRows()}
      </div>

      <div class="overview-footer">
        <div>Total Tables: <strong>${visibleTables.length}</strong>${hasTableSearch ? ` <span class="muted-inline">(filtered from ${state.tableOverview.length})</span>` : ''}</div>
        <div>Total Rows: <strong>${Number.isFinite(totalRows) ? totalRows.toLocaleString() : '-'}</strong></div>
        <div>Connected as <strong>${esc(state.activeConnection?.name || '-')}</strong></div>
      </div>
    </section>
  `
}

function renderTableTabs() {
  if (!state.tableTabs.length) {
    return '<div class="empty-small">Open a table from the left panel or overview.</div>'
  }

  return state.tableTabs
    .map((tab) => {
      const active = tab.id === state.activeTableTab
      return `
        <div class="table-tab ${active ? 'active' : ''}">
          <button class="table-tab-main" data-action="activate-table-tab" data-id="${esc(tab.id)}">${esc(tab.schema)}.${esc(tab.table)}</button>
          <button class="table-tab-close" data-action="close-table-tab" data-id="${esc(tab.id)}">x</button>
        </div>
      `
    })
    .join('')
}

function renderSortIndicator(column, tableState) {
  if (!tableState || tableState.sortColumn !== column) {
    return ''
  }
  return tableState.sortDirection === 'DESC' ? 'v' : '^'
}

function renderTableRows(tableState) {
  if (!tableState) {
    return '<div class="empty-row">Open a table to view data.</div>'
  }
  if (tableState.loading) {
    return loadingMarkup('Loading table data...')
  }
  if (tableState.error) {
    return `<div class="empty-row">${esc(tableState.error)}</div>`
  }
  if (!tableState.rows.length) {
    return '<div class="empty-row">No rows matched this filter.</div>'
  }

  return tableState.rows
    .map((row, rowIndex) => {
      const selected = rowIndex === tableState.selectedRow
      const template = gridTemplateFromColumns(tableState.columns, tableState.columnWidths)
      const cells = tableState.columns
        .map((col, colIndex) => {
          const raw = row[colIndex]
          const fullValue = cellTextValue(raw)
          const previewValue = cellPreviewValue(raw)
          const showViewer = shouldShowCellViewer(raw)
          const isInlineCell = state.inlineCell.open
            && state.inlineCell.tabId === state.activeTableTab
            && state.inlineCell.rowIndex === rowIndex
            && state.inlineCell.colIndex === colIndex

          if (isInlineCell) {
            return `
              <div class="data-cell data-cell-editing" data-row-index="${rowIndex}" data-col-index="${colIndex}" title="${esc(fullValue)}">
                <textarea
                  class="data-cell-inline-editor"
                  data-cell-editor="true"
                  data-row-index="${rowIndex}"
                  data-col-index="${colIndex}"
                  readonly
                >${esc(fullValue)}</textarea>
              </div>
            `
          }

          return `
            <div class="data-cell ${showViewer ? 'has-view-btn' : ''}" data-row-index="${rowIndex}" data-col-index="${colIndex}" title="${esc(fullValue)}">
              <div class="data-cell-content">${esc(previewValue)}</div>
              ${showViewer ? `<button class="mini-btn data-cell-view-btn" data-action="open-cell-view" data-row-index="${rowIndex}" data-col-index="${colIndex}">View</button>` : ''}
            </div>
          `
        })
        .join('')
      return `
        <div class="data-row ${selected ? 'selected' : ''}" data-action="select-row" data-index="${rowIndex}" data-table-grid="active" style="grid-template-columns:${template};">
          ${cells}
        </div>
      `
    })
    .join('')
}

function renderTableView() {
  const tableState = currentTableState()
  const table = state.tableTabs.find((item) => item.id === state.activeTableTab)

  if (!tableState || !table) {
    return `
      <section class="workspace-view">
        <div class="section-title">Table Data</div>
        <div class="empty-row">Open a table from Explorer to view rows.</div>
      </section>
    `
  }

  const totalPages = Math.max(1, Math.ceil((Number(tableState.totalRows) || 0) / Math.max(1, Number(tableState.limit))))
  const tableTemplate = gridTemplateFromColumns(tableState.columns, tableState.columnWidths)

  return `
    <section class="workspace-view">
      <div class="table-tabs-strip">${renderTableTabs()}</div>

      <div class="workspace-head table-head-tight">
        <div>
          <div class="crumbs">${esc(table.schema)} &gt; ${esc(table.table)}</div>
          <h2 class="section-title">${esc(table.table)}</h2>
        </div>
        <div class="workspace-head-actions wrap">
          <button class="btn btn-secondary compact" data-action="table-refresh">Refresh</button>
          <button class="btn btn-secondary compact" data-action="table-insert">Insert</button>
          <button class="btn btn-secondary compact" data-action="table-update">Update</button>
          <button class="btn btn-secondary compact" data-action="table-delete">Delete</button>
        </div>
      </div>

      <div class="data-controls">
        <label class="control-block">
          <span>Filter</span>
          <input type="text" data-table-field="filterDraft" value="${esc(tableState.filterDraft)}" placeholder="SQL WHERE clause or Mongo JSON" />
        </label>
        <label class="control-block">
          <span>Sort</span>
          <input type="text" data-table-field="sortDraft" value="${esc(tableState.sortDraft)}" placeholder="created_at DESC" />
        </label>
        <label class="control-block small">
          <span>Rows</span>
          <select data-table-field="limit">
            ${[25, 50, 100, 250].map((n) => `<option value="${n}" ${Number(tableState.limit) === n ? 'selected' : ''}>${n}</option>`).join('')}
          </select>
        </label>
        <div class="control-actions">
          <button class="mini-btn" data-action="table-apply-filter">Apply</button>
          <button class="mini-btn" data-action="table-clear-filter">Clear</button>
        </div>
      </div>

      <div class="data-meta">
        Showing ${tableState.from || 0}-${tableState.to || 0} of ${(Number(tableState.totalRows) || 0).toLocaleString()} records
      </div>

      <div class="data-grid">
        <div class="data-head" data-table-grid="active" style="grid-template-columns:${tableTemplate};">
          ${tableState.columns
            .map(
              (col) => `
                <div class="data-head-cell">
                  <button class="data-sort-btn" data-action="sort-column" data-column="${esc(col)}">
                    <span>${esc(col)}</span>
                    <span class="sort-indicator">${renderSortIndicator(col, tableState)}</span>
                  </button>
                  <span
                    class="col-resize-handle"
                    data-action="start-col-resize"
                    data-resize-target="table"
                    data-column="${esc(col)}"
                    title="Drag to resize column"
                  ></span>
                </div>
              `,
            )
            .join('')}
        </div>
        ${renderTableRows(tableState)}
      </div>

      <div class="pager table-pager">
        <button class="pager-btn" data-action="table-prev" ${tableState.page <= 1 ? 'disabled' : ''}>Previous</button>
        <span class="page-chip">${tableState.page}</span>
        <span class="page-text">/ ${totalPages}</span>
        <button class="pager-btn" data-action="table-next" ${tableState.page >= totalPages ? 'disabled' : ''}>Next</button>
      </div>
    </section>
  `
}

function renderSnippetPanel() {
  const items = state.queryPanel === 'snippets' ? state.snippets : state.history

  if (!items.length) {
    return '<div class="empty-small">No items yet.</div>'
  }

  if (state.queryPanel === 'snippets') {
    return items
      .map((item) => {
        return `
          <div class="snippet-item">
            <button class="snippet-main" data-action="load-snippet" data-id="${esc(item.id)}">
              <div class="snippet-name">${esc(item.name)}</div>
              <div class="snippet-meta">${esc(formatTime(item.updated_at || item.created_at))}</div>
            </button>
            <button class="mini-btn danger" data-action="delete-snippet" data-id="${esc(item.id)}">Delete</button>
          </div>
        `
      })
      .join('')
  }

  return items
    .map((item) => {
      return `
        <div class="history-item">
          <div class="history-query">${esc(String(item.query || '').slice(0, 120))}</div>
          <div class="history-meta">${esc(formatTime(item.executed_at))} | ${esc(item.duration_ms || 0)} ms ${item.error ? '| Failed' : '| OK'}</div>
        </div>
      `
    })
    .join('')
}

function renderQueryResults() {
  const result = state.queryResult

  if (state.runningQuery) {
    return loadingMarkup('Running query...')
  }

  if (!result.columns.length && !result.rows.length && !result.message) {
    return '<div class="empty-row">Run a query to see results.</div>'
  }

  const resultTemplate = gridTemplateFromColumns(result.columns, state.queryResultColumnWidths)
  const heads = result.columns
    .map(
      (col) => `
        <div class="result-head-cell">
          <span>${esc(col)}</span>
          <span
            class="col-resize-handle"
            data-action="start-col-resize"
            data-resize-target="query"
            data-column="${esc(col)}"
            title="Drag to resize column"
          ></span>
        </div>
      `,
    )
    .join('')
  const rows = result.rows
    .map((row) => {
      const values = result.columns.map((_, index) => row?.[index] ?? '')
      return `<div class="result-row" data-query-grid="active" style="grid-template-columns:${resultTemplate};">${values.map((v) => `<div>${esc(v)}</div>`).join('')}</div>`
    })
    .join('')

  return `
    <div class="result-meta">${esc(result.message || '')} ${result.rowsAffected ? `| ${result.rowsAffected} row(s) affected` : ''} ${result.durationMs ? `| ${result.durationMs} ms` : ''}</div>
    ${result.columns.length ? `<div class="result-head" data-query-grid="active" style="grid-template-columns:${resultTemplate};">${heads}</div>` : ''}
    ${rows || '<div class="empty-row">No rows returned.</div>'}
  `
}

function renderQueryView() {
  const hasQueryResult = !!(state.queryResult.columns?.length || state.queryResult.rows?.length)
  return `
    <section class="workspace-view query-view">
      <div class="query-toolbar">
        <button class="btn btn-primary compact" data-action="run-query" ${state.runningQuery ? 'disabled' : ''}>${state.runningQuery ? 'Running...' : 'Run Selection'}</button>
        <button class="btn btn-secondary compact" data-action="format-query">Format SQL</button>
        <button class="btn btn-secondary compact" data-action="save-snippet">Save Snippet</button>
        <button class="btn btn-secondary compact" data-action="export-query-csv" ${hasQueryResult ? '' : 'disabled'}>Export CSV</button>
        <button class="btn btn-secondary compact" data-action="export-query-json" ${hasQueryResult ? '' : 'disabled'}>Export JSON</button>
      </div>

      <div class="query-layout">
        <div class="query-editor-wrap">
          <textarea id="query-editor" spellcheck="false">${esc(state.queryEditor)}</textarea>

          <div class="query-results">
            <div class="results-title">Results</div>
            <div class="results-grid">${renderQueryResults()}</div>
          </div>
        </div>

        <aside class="query-side">
          <div class="query-side-tabs">
            <button class="mini-btn ${state.queryPanel === 'snippets' ? 'active' : ''}" data-action="query-panel" data-panel="snippets">Snippets</button>
            <button class="mini-btn ${state.queryPanel === 'history' ? 'active' : ''}" data-action="query-panel" data-panel="history">History</button>
          </div>
          <div class="query-side-list">${renderSnippetPanel()}</div>
        </aside>
      </div>
    </section>
  `
}

function renderWorkspaceMain() {
  let content = renderExplorerView()
  if (state.workspaceTab === 'table') {
    content = renderTableView()
  }
  if (state.workspaceTab === 'query') {
    content = renderQueryView()
  }

  return `
    <div class="layout-grid workspace-grid">
      ${renderWorkspaceSidebar()}
      <main class="page workspace-page">
        <div class="workspace-tabs">
          <button class="workspace-tab ${state.workspaceTab === 'explorer' ? 'active' : ''}" data-action="switch-workspace" data-view="explorer">Explorer</button>
          <button class="workspace-tab ${state.workspaceTab === 'table' ? 'active' : ''}" data-action="switch-workspace" data-view="table">Table Data</button>
          <button class="workspace-tab ${state.workspaceTab === 'query' ? 'active' : ''}" data-action="switch-workspace" data-view="query">Query Studio</button>
        </div>
        ${content}
      </main>
    </div>
  `
}

function renderRowFieldControl(meta, modal) {
  const name = meta.name
  const dataType = meta.dataType
  const value = String(modal.fields[name] ?? '')
  const disabled = modal.busy || !!modal.nulls[name]
  const disabledAttr = disabled ? 'disabled' : ''
  const placeholder = meta.defaultValue ? `Default: ${meta.defaultValue}` : ''

  if ((meta.enumValues || []).length) {
    const options = [
      modal.nulls[name] ? '' : value,
      ...meta.enumValues,
    ]
    const unique = [...new Set(options)].filter((v) => v !== '')
    return `
      <select class="row-input-control" data-row-field="${esc(name)}" ${disabledAttr}>
        <option value="">Select value</option>
        ${unique
          .map((item) => `<option value="${esc(item)}" ${item === value ? 'selected' : ''}>${esc(item)}</option>`)
          .join('')}
      </select>
    `
  }

  if (isBooleanType(dataType)) {
    return `
      <select class="row-input-control" data-row-field="${esc(name)}" ${disabledAttr}>
        <option value="" ${value === '' ? 'selected' : ''}>Select</option>
        <option value="true" ${value === 'true' ? 'selected' : ''}>true</option>
        <option value="false" ${value === 'false' ? 'selected' : ''}>false</option>
      </select>
    `
  }

  if (isDateTimeType(dataType)) {
    return `<input class="row-input-control" type="datetime-local" data-row-field="${esc(name)}" value="${esc(value)}" ${disabledAttr} />`
  }
  if (isDateType(dataType)) {
    return `<input class="row-input-control" type="date" data-row-field="${esc(name)}" value="${esc(value)}" ${disabledAttr} />`
  }
  if (isTimeType(dataType)) {
    return `<input class="row-input-control" type="time" step="1" data-row-field="${esc(name)}" value="${esc(value)}" ${disabledAttr} />`
  }
  if (isNumericType(dataType)) {
    const step = isIntegerType(dataType) ? '1' : 'any'
    return `<input class="row-input-control" type="number" step="${step}" data-row-field="${esc(name)}" value="${esc(value)}" placeholder="${esc(placeholder)}" ${disabledAttr} />`
  }
  if (isJSONType(dataType)) {
    return `<textarea class="row-input-control row-input-textarea" data-row-field="${esc(name)}" rows="3" placeholder='{"key":"value"}' ${disabledAttr}>${esc(value)}</textarea>`
  }

  return `<input class="row-input-control" type="text" data-row-field="${esc(name)}" value="${esc(value)}" placeholder="${esc(placeholder)}" ${disabledAttr} />`
}

function renderRowFieldCard(meta, modal) {
  const name = meta.name
  const dataType = meta.dataType
  const hints = []
  if (meta.isPrimary) {
    hints.push('Primary Key')
  }
  hints.push(meta.nullable ? 'Nullable' : 'Not Null')
  if (meta.defaultValue) {
    hints.push(`Default: ${meta.defaultValue}`)
  }
  if ((meta.enumValues || []).length) {
    hints.push(`Enum: ${meta.enumValues.join(', ')}`)
  }

  const actions = []
  if (isUUIDType(dataType)) {
    actions.push(`<button class="mini-btn" data-action="row-field-generate-uuid" data-column="${esc(name)}" ${modal.busy ? 'disabled' : ''}>Generate UUID</button>`)
  }
  if (isDateType(dataType) || isDateTimeType(dataType) || isTimeType(dataType)) {
    actions.push(`<button class="mini-btn" data-action="row-field-set-now" data-column="${esc(name)}" data-type="${esc(dataType)}" ${modal.busy ? 'disabled' : ''}>Set to Now</button>`)
  }
  if (meta.nullable) {
    actions.push(
      `<button class="mini-btn ${modal.nulls[name] ? 'active' : ''}" data-action="row-field-null-toggle" data-column="${esc(name)}" ${modal.busy ? 'disabled' : ''}>${modal.nulls[name] ? 'Unset NULL' : 'Set NULL'}</button>`,
    )
  }

  return `
    <div class="row-field-card ${modal.nulls[name] ? 'is-null' : ''}">
      <div class="row-field-head">
        <div class="row-field-title-wrap">
          <div class="row-field-icon">${esc(rowFieldIcon(meta))}</div>
          <div>
            <div class="row-field-title-line">
              <div class="row-field-name">${esc(name)}</div>
              <span class="row-type-pill">${esc(typeBadge(dataType))}</span>
            </div>
            <div class="row-field-meta">${hints.map((hint) => `<span>${esc(hint)}</span>`).join('<span class="dot">•</span>')}</div>
          </div>
        </div>
        ${actions.length ? `<div class="row-field-actions">${actions.join('')}</div>` : ''}
      </div>
      <div class="row-field-input">
        ${renderRowFieldControl(meta, modal)}
      </div>
      ${modal.nulls[name] ? '<div class="row-null-hint">Value will be saved as NULL</div>' : ''}
    </div>
  `
}

function renderRowModalKeySection(modal, columns) {
  if (modal.mode !== 'update' && modal.mode !== 'delete') {
    return ''
  }
  const keyMeta = columns.find((column) => column.name === modal.keyColumn)
  const keyType = keyMeta ? typeBadge(keyMeta.dataType) : 'TEXT'
  return `
    <div class="row-key-grid">
      <label class="field">
        <span>Key Column</span>
        <select data-row-field="__keyColumn" ${modal.busy ? 'disabled' : ''}>
          ${columns.map((column) => `<option value="${esc(column.name)}" ${modal.keyColumn === column.name ? 'selected' : ''}>${esc(column.name)}</option>`).join('')}
        </select>
      </label>
      <label class="field">
        <span>Key Value (${esc(keyType)})</span>
        <input type="text" data-row-field="__keyValue" value="${esc(modal.keyValue)}" ${modal.busy ? 'disabled' : ''} />
      </label>
    </div>
  `
}

function renderStructureModal() {
  const modal = state.structureModal
  if (!modal.open) {
    return ''
  }

  const rows = modal.loading
    ? loadingMarkup('Loading table structure...')
    : modal.error
      ? `<div class="empty-row">${esc(modal.error)}</div>`
      : !modal.columns.length
        ? '<div class="empty-row">No columns found.</div>'
        : modal.columns
            .map((column) => {
              const flags = []
              if (column.isPrimary) {
                flags.push('PK')
              }
              if (!column.nullable) {
                flags.push('NOT NULL')
              }
              if (column.defaultValue) {
                flags.push(`DEFAULT ${column.defaultValue}`)
              }
              return `
                <div class="structure-row">
                  <div class="structure-col-name">${esc(column.name)}</div>
                  <div class="structure-col-type">${esc(typeBadge(column.dataType))}</div>
                  <div class="structure-col-flags">${flags.map((f) => `<span>${esc(f)}</span>`).join('')}</div>
                </div>
              `
            })
            .join('')

  return `
    <div class="overlay" data-action="structure-close-bg">
      <div class="structure-modal">
        <div class="drawer-head">
          <h3>Table Structure</h3>
          <button class="btn btn-icon" data-action="structure-close">x</button>
        </div>
        <div class="structure-subline">${esc(modal.schema || '-')} . ${esc(modal.table || '-')}</div>
        <div class="structure-body">
          <div class="structure-head">
            <div>Column</div>
            <div>Type</div>
            <div>Flags</div>
          </div>
          <div class="structure-list">${rows}</div>
        </div>
        <div class="drawer-foot">
          <button class="btn btn-secondary" data-action="structure-close">Close</button>
          <button class="btn btn-primary" data-action="open-table" data-schema="${esc(modal.schema)}" data-table="${esc(modal.table)}">Browse Table</button>
        </div>
      </div>
    </div>
  `
}

function renderRowModal() {
  const modal = state.rowModal
  if (!modal.open) {
    return ''
  }

  const tab = state.tableTabs.find((item) => item.id === modal.tabId)
  const tableState = state.tableStateByTab[modal.tabId]
  if (!tableState || !tab) {
    return ''
  }

  const columns = modal.columns?.length ? modal.columns : rowModalColumns(modal.tabId, tableState.columns)
  const modeTitle = modal.mode === 'insert' ? `Insert Row into ${tab.table}` : modal.mode === 'update' ? `Update Row in ${tab.table}` : `Delete Row from ${tab.table}`
  const isDelete = modal.mode === 'delete'
  const bodyContent = isDelete
    ? `
      <div class="warning-box">This action will permanently delete one row.</div>
      ${renderRowModalKeySection(modal, columns)}
    `
    : modal.loading
      ? loadingMarkup('Loading column types...')
      : `
        ${renderRowModalKeySection(modal, columns)}
        <div class="row-form-list">
          ${columns.length ? columns.map((column) => renderRowFieldCard(column, modal)).join('') : '<div class="empty-row">No columns found for this table.</div>'}
        </div>
      `

  return `
    <div class="overlay" data-action="row-modal-close-bg">
      <div class="row-modal">
        <div class="drawer-head">
          <h3>${modeTitle}</h3>
          <button class="btn btn-icon" data-action="row-modal-close">x</button>
        </div>

        <div class="row-modal-body">
          <div class="row-modal-subline">Database: <strong>${esc(tab.schema || '-')}</strong> | Table: <strong>${esc(tab.table)}</strong></div>
          ${bodyContent}
        </div>

        <div class="drawer-foot">
          <button class="btn btn-secondary" data-action="row-modal-close" ${modal.busy ? 'disabled' : ''}>Cancel</button>
          <button class="btn ${isDelete ? 'btn-danger' : 'btn-primary'}" data-action="row-modal-submit" ${modal.busy || modal.loading ? 'disabled' : ''}>${modal.busy ? 'Working...' : modal.mode === 'insert' ? 'Insert Row' : modal.mode === 'update' ? 'Update Row' : 'Delete Row'}</button>
        </div>
      </div>
    </div>
  `
}

function renderCellViewerModal() {
  const viewer = state.cellViewer
  if (!viewer.open) {
    return ''
  }

  const table = state.tableTabs.find((item) => item.id === viewer.tabId)
  const tableState = state.tableStateByTab[viewer.tabId]
  const colName = tableState?.columns?.[viewer.colIndex] || `Column ${viewer.colIndex + 1}`
  const body = viewer.isJSON ? viewer.prettyValue : viewer.value

  return `
    <div class="overlay" data-action="cell-view-close-bg">
      <div class="cell-viewer-modal">
        <div class="drawer-head">
          <h3>Cell Value</h3>
          <button class="btn btn-icon" data-action="cell-view-close">x</button>
        </div>
        <div class="cell-view-meta">
          <div>Table: <strong>${esc(table ? `${table.schema}.${table.table}` : '-')}</strong></div>
          <div>Column: <strong>${esc(colName)}</strong></div>
          ${viewer.isJSON ? '<span class="cell-view-badge">JSON decoded</span>' : ''}
        </div>
        <div class="cell-view-body">
          <pre class="cell-view-content ${viewer.isJSON ? 'json' : ''}">${esc(body || '(empty)')}</pre>
        </div>
        <div class="drawer-foot">
          <button class="btn btn-secondary" data-action="cell-view-close">Close</button>
        </div>
      </div>
    </div>
  `
}

function renderToast() {
  if (!state.toast) {
    return ''
  }
  return `<div class="toast ${state.toast.error ? 'error' : ''}">${esc(state.toast.message)}</div>`
}

function render() {
  applyThemeClass()

  const bodyContent = state.route === 'workspace'
    ? renderWorkspaceMain()
    : state.route === 'manager'
      ? renderConnectionManagerView()
      : renderConnectionsView()

  root.innerHTML = `
    <div class="app-shell">
      ${renderTopBar()}
      ${bodyContent}
      ${renderStructureModal()}
      ${renderRowModal()}
      ${renderCellViewerModal()}
      ${renderToast()}
    </div>
  `
}

function renderWithInputFocus(selector, selectionStart = null, selectionEnd = null) {
  render()
  requestAnimationFrame(() => {
    const input = root.querySelector(selector)
    if (!(input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement)) {
      return
    }
    input.focus()
    const valueLength = input.value.length
    const start = Number.isFinite(selectionStart) ? Math.max(0, Math.min(Number(selectionStart), valueLength)) : valueLength
    const end = Number.isFinite(selectionEnd) ? Math.max(start, Math.min(Number(selectionEnd), valueLength)) : start
    try {
      input.setSelectionRange(start, end)
    } catch {
      // setSelectionRange can fail for some input modes
    }
  })
}

function updateConnectionFormField(field, rawValue, isCheckbox = false) {
  const value = isCheckbox ? !!rawValue : String(rawValue ?? '')
  const wasUnsaved = state.manager.unsaved
  state.manager.form[field] = value
  state.manager.unsaved = true

  let needsRender = false
  if (field === 'type' && !state.manager.form.useConnString) {
    state.manager.form.port = String(defaultPortByType(value))
    needsRender = true
  }
  if (field === 'useConnString' || field === 'mode' || field === 'sshAuthType') {
    needsRender = true
  }

  if (needsRender) {
    render()
    return
  }

  if (!wasUnsaved && state.manager.unsaved) {
    const badge = root.querySelector('.manager-unsaved')
    if (badge) {
      badge.classList.add('show')
    }
  }
}

function updateTableField(field, rawValue) {
  const tableState = currentTableState()
  if (!tableState) {
    return
  }

  if (field === 'limit') {
    tableState.limit = Number(rawValue || 50)
    tableState.page = 1
    loadTableData(state.activeTableTab)
    return
  }

  tableState[field] = String(rawValue ?? '')
}

function handleMouseDown(event) {
  if (!(event.target instanceof Element)) {
    return
  }
  const target = event.target.closest('[data-action="start-col-resize"]')
  if (!target) {
    return
  }

  event.preventDefault()
  event.stopPropagation()

  const resizeTarget = target.dataset.resizeTarget || 'table'
  const column = target.dataset.column || ''
  if (!column) {
    return
  }

  if (resizeTarget === 'table') {
    const tableState = currentTableState()
    if (!tableState) {
      return
    }
    const current = Number(tableState.columnWidths?.[column] || 180)
    state.resizing = {
      active: true,
      target: 'table',
      tabId: state.activeTableTab,
      column,
      startX: event.clientX,
      startWidth: current,
    }
  } else {
    const current = Number(state.queryResultColumnWidths?.[column] || 180)
    state.resizing = {
      active: true,
      target: 'query',
      tabId: '',
      column,
      startX: event.clientX,
      startWidth: current,
    }
  }
  document.body.classList.add('resizing-columns')
}

function handleMouseMove(event) {
  if (!state.resizing.active) {
    return
  }
  const delta = event.clientX - state.resizing.startX
  const nextWidth = Math.max(100, Math.min(920, Math.round(state.resizing.startWidth + delta)))

  if (state.resizing.target === 'table') {
    const tableState = state.tableStateByTab[state.resizing.tabId]
    if (!tableState) {
      return
    }
    tableState.columnWidths[state.resizing.column] = nextWidth
    applyActiveTableGridTemplate(state.resizing.tabId)
    return
  }

  state.queryResultColumnWidths[state.resizing.column] = nextWidth
  applyQueryGridTemplate()
}

function handleMouseUp() {
  if (!state.resizing.active) {
    return
  }
  state.resizing.active = false
  state.resizing.target = ''
  state.resizing.tabId = ''
  state.resizing.column = ''
  document.body.classList.remove('resizing-columns')
}

function handleDoubleClick(event) {
  if (!(event.target instanceof Element)) {
    return
  }

  if (event.target.closest('[data-action="open-cell-view"]')) {
    return
  }

  const cell = event.target.closest('.data-cell')
  if (!cell) {
    return
  }

  const rowIndex = Number(cell.dataset.rowIndex ?? -1)
  const colIndex = Number(cell.dataset.colIndex ?? -1)
  const tableState = currentTableState()
  if (!tableState || !Number.isInteger(rowIndex) || !Number.isInteger(colIndex)) {
    return
  }
  if (rowIndex < 0 || rowIndex >= tableState.rows.length || colIndex < 0 || colIndex >= tableState.columns.length) {
    return
  }

  state.inlineCell = {
    open: true,
    tabId: state.activeTableTab,
    rowIndex,
    colIndex,
  }
  closeCellViewer()
  render()
  focusInlineCellEditor()
}

function handleClick(event) {
  if (!(event.target instanceof Element)) {
    return
  }

  const clickTarget = event.target
  const target = clickTarget.closest('[data-action]')
  if (!target || (String(target.dataset.action || '').endsWith('-close-bg') && target !== clickTarget)) {
    if (state.inlineCell.open && !clickTarget.closest('[data-cell-editor="true"]')) {
      clearInlineCell()
      render()
    }
    return
  }

  const action = target.dataset.action
  const id = target.dataset.id || ''

  switch (action) {
    case 'new-connection':
      openNewConnectionDrawer()
      break

    case 'manager-new':
      openNewConnectionDrawer()
      break

    case 'manager-back':
      state.route = 'connections'
      render()
      break

    case 'manager-select-connection':
      selectConnectionInManager(id)
      break

    case 'manager-mode':
      state.manager.form.mode = target.dataset.mode === 'ssh' ? 'ssh' : 'direct'
      state.manager.unsaved = true
      render()
      break

    case 'manager-auth':
      state.manager.form.sshAuthType = target.dataset.auth || 'password'
      state.manager.unsaved = true
      render()
      break

    case 'manager-pick-key':
      pickManagerSSHKeyFile()
      break

    case 'manager-save':
      saveDrawerConnection()
      break

    case 'manager-test':
      testDrawerConnection()
      break

    case 'manager-delete':
      deleteManagerConnection()
      break

    case 'manager-open-selected':
      if (state.manager.form.id) {
        openConnection(state.manager.form.id)
      } else {
        notify('Select a connection first', true)
      }
      break

    case 'refresh-connections':
      loadConnections()
      break

    case 'filter-connections':
      notify('Use search to filter connections')
      break

    case 'connections-prev':
      if (state.connectionPage > 1) {
        state.connectionPage -= 1
        loadConnections()
      }
      break

    case 'connections-page': {
      const next = Number(target.dataset.page || state.connectionPage)
      if (Number.isFinite(next) && next >= 1 && next <= state.connectionTotalPages) {
        state.connectionPage = next
        loadConnections()
      }
      break
    }

    case 'connections-next':
      if (state.connectionPage < state.connectionTotalPages) {
        state.connectionPage += 1
        loadConnections()
      }
      break

    case 'open-connection':
      openConnection(id)
      break

    case 'edit-connection':
      openEditConnectionDrawer(id)
      break

    case 'delete-connection':
      deleteConnection(id)
      break

    case 'go-connections':
      state.route = 'connections'
      loadConnections()
      render()
      break

    case 'disconnect-active':
      disconnectActive()
      break

    case 'switch-workspace':
      state.workspaceTab = target.dataset.view || 'explorer'
      if (state.workspaceTab !== 'table') {
        clearCellInteractions(state.activeTableTab)
      }
      render()
      break

    case 'select-schema':
      state.selectedSchema = target.dataset.schema || ''
      loadTableOverview()
      render()
      break

    case 'refresh-overview':
      loadTableOverview()
      break

    case 'create-table':
      createTableFromOverview()
      break

    case 'export-database':
      exportDatabaseFromOverview()
      break

    case 'table-browse': {
      const schema = target.dataset.schema || state.selectedSchema
      const table = target.dataset.table || ''
      if (table) {
        clearInlineCell()
        closeCellViewer()
        openTableTab(schema, table)
      }
      break
    }

    case 'table-structure': {
      const schema = target.dataset.schema || state.selectedSchema
      const table = target.dataset.table || ''
      if (table) {
        openStructureModal(schema, table)
      }
      break
    }

    case 'table-empty': {
      const schema = target.dataset.schema || state.selectedSchema
      const table = target.dataset.table || ''
      if (table) {
        emptyTableFromOverview(schema, table)
      }
      break
    }

    case 'table-drop': {
      const schema = target.dataset.schema || state.selectedSchema
      const table = target.dataset.table || ''
      if (table) {
        dropTableFromOverview(schema, table)
      }
      break
    }

    case 'structure-close-bg':
    case 'structure-close':
      closeStructureModal()
      break

    case 'open-table': {
      const schema = target.dataset.schema || state.selectedSchema
      const table = target.dataset.table || ''
      if (table) {
        if (state.structureModal.open) {
          closeStructureModal()
        }
        clearInlineCell()
        closeCellViewer()
        openTableTab(schema, table)
      }
      break
    }

    case 'activate-table-tab':
      state.activeTableTab = id
      state.workspaceTab = 'table'
      clearCellInteractions()
      render()
      break

    case 'close-table-tab':
      closeTableTab(id)
      break

    case 'table-refresh':
      loadTableData(state.activeTableTab)
      break

    case 'table-prev': {
      const tableState = currentTableState()
      if (tableState && tableState.page > 1) {
        tableState.page -= 1
        loadTableData(state.activeTableTab)
      }
      break
    }

    case 'table-next': {
      const tableState = currentTableState()
      if (tableState) {
        const totalPages = Math.max(1, Math.ceil((Number(tableState.totalRows) || 0) / Math.max(1, Number(tableState.limit))))
        if (tableState.page < totalPages) {
          tableState.page += 1
          loadTableData(state.activeTableTab)
        }
      }
      break
    }

    case 'table-apply-filter': {
      const tableState = currentTableState()
      if (!tableState) {
        break
      }
      tableState.filter = tableState.filterDraft
      tableState.sort = tableState.sortDraft
      tableState.page = 1
      tableState.sortColumn = ''
      tableState.sortDirection = ''
      clearCellInteractions(state.activeTableTab)
      loadTableData(state.activeTableTab)
      break
    }

    case 'table-clear-filter': {
      const tableState = currentTableState()
      if (!tableState) {
        break
      }
      tableState.filterDraft = ''
      tableState.filter = ''
      tableState.sortDraft = ''
      tableState.sort = ''
      tableState.sortColumn = ''
      tableState.sortDirection = ''
      tableState.page = 1
      clearCellInteractions(state.activeTableTab)
      loadTableData(state.activeTableTab)
      break
    }

    case 'sort-column':
      clearCellInteractions(state.activeTableTab)
      setTableSort(target.dataset.column || '')
      break

    case 'select-row': {
      const tableState = currentTableState()
      if (tableState) {
        clearInlineCell()
        closeCellViewer()
        tableState.selectedRow = Number(target.dataset.index || -1)
        render()
      }
      break
    }

    case 'open-cell-view': {
      const rowIndex = Number(target.dataset.rowIndex || -1)
      const colIndex = Number(target.dataset.colIndex || -1)
      openCellViewerAt(rowIndex, colIndex)
      break
    }

    case 'cell-view-close-bg':
    case 'cell-view-close':
      closeCellViewer()
      render()
      break

    case 'table-insert':
      clearCellInteractions(state.activeTableTab)
      openRowModal('insert')
      break

    case 'table-update':
      clearCellInteractions(state.activeTableTab)
      openRowModal('update')
      break

    case 'table-delete':
      clearCellInteractions(state.activeTableTab)
      openRowModal('delete')
      break

    case 'row-modal-close-bg':
    case 'row-modal-close':
      state.rowModal.open = false
      state.rowModal.busy = false
      state.rowModal.loading = false
      render()
      break

    case 'row-field-null-toggle': {
      const column = target.dataset.column || ''
      if (!column) {
        break
      }
      const nextNull = !state.rowModal.nulls[column]
      state.rowModal.nulls[column] = nextNull
      if (nextNull) {
        state.rowModal.fields[column] = ''
      }
      if (state.rowModal.keyColumn === column) {
        state.rowModal.keyValue = nextNull ? '' : String(state.rowModal.fields[column] ?? '')
      }
      render()
      break
    }

    case 'row-field-set-now': {
      const column = target.dataset.column || ''
      if (!column) {
        break
      }
      const dataType = target.dataset.type || ''
      state.rowModal.fields[column] = nowValueForType(dataType)
      state.rowModal.nulls[column] = false
      if (state.rowModal.keyColumn === column) {
        state.rowModal.keyValue = String(state.rowModal.fields[column] ?? '')
      }
      render()
      break
    }

    case 'row-field-generate-uuid': {
      const column = target.dataset.column || ''
      if (!column) {
        break
      }
      state.rowModal.fields[column] = randomUUID()
      state.rowModal.nulls[column] = false
      if (state.rowModal.keyColumn === column) {
        state.rowModal.keyValue = state.rowModal.fields[column]
      }
      render()
      break
    }

    case 'row-modal-submit':
      submitRowModal()
      break

    case 'run-query':
      runQueryFromEditor()
      break

    case 'format-query':
      formatQueryInEditor()
      break

    case 'save-snippet':
      saveSnippetFromEditor()
      break

    case 'export-query-csv':
      exportQueryResults('csv')
      break

    case 'export-query-json':
      exportQueryResults('json')
      break

    case 'query-panel':
      state.queryPanel = target.dataset.panel === 'history' ? 'history' : 'snippets'
      render()
      break

    case 'load-snippet': {
      const snippet = state.snippets.find((item) => item.id === id)
      if (snippet) {
        state.queryEditor = snippet.query || ''
        render()
      }
      break
    }

    case 'delete-snippet':
      deleteSnippet(id)
      break

    default:
      break
  }
}

function handleInput(event) {
  const target = event.target

  if (target instanceof HTMLInputElement || target instanceof HTMLSelectElement || target instanceof HTMLTextAreaElement) {
    if (target.id === 'global-search') {
      const selStart = target.selectionStart
      const selEnd = target.selectionEnd
      if (state.route === 'manager') {
        state.manager.search = target.value
        renderWithInputFocus('#global-search', selStart, selEnd)
        return
      }
      if (state.route === 'workspace') {
        state.tableListSearch = target.value
        renderWithInputFocus('#global-search', selStart, selEnd)
        return
      }

      state.connectionSearch = target.value
      state.connectionPage = 1
      if (searchTimer) {
        clearTimeout(searchTimer)
      }
      searchTimer = setTimeout(() => {
        if (state.route === 'connections') {
          loadConnections()
        }
      }, 220)
      return
    }

    if (target.id === 'theme-select') {
      state.theme = target.value === 'light' ? 'light' : 'dark'
      render()
      return
    }

    if (target.id === 'query-editor') {
      state.queryEditor = target.value
      return
    }

    if (target.dataset.connField) {
      const field = target.dataset.connField
      updateConnectionFormField(field, target instanceof HTMLInputElement && target.type === 'checkbox' ? target.checked : target.value, target instanceof HTMLInputElement && target.type === 'checkbox')
      return
    }

    if (target.dataset.tableField) {
      updateTableField(target.dataset.tableField, target.value)
      return
    }

    if (target.dataset.rowField) {
      const field = target.dataset.rowField
      if (field === '__keyColumn') {
        state.rowModal.keyColumn = target.value
        state.rowModal.keyValue = state.rowModal.nulls[target.value] ? '' : String(state.rowModal.fields[target.value] ?? state.rowModal.keyValue)
        render()
      } else if (field === '__keyValue') {
        state.rowModal.keyValue = target.value
      } else {
        state.rowModal.fields[field] = target.value
        if (String(target.value ?? '') !== '') {
          state.rowModal.nulls[field] = false
        }
        if (state.rowModal.keyColumn === field && !state.rowModal.nulls[field]) {
          state.rowModal.keyValue = String(target.value ?? '')
        }
      }
      return
    }
  }
}

async function init() {
  root.addEventListener('click', handleClick)
  root.addEventListener('dblclick', handleDoubleClick)
  root.addEventListener('input', handleInput)
  root.addEventListener('change', handleInput)
  root.addEventListener('mousedown', handleMouseDown)
  window.addEventListener('mousemove', handleMouseMove)
  window.addEventListener('mouseup', handleMouseUp)

  render()
  await loadConnections()

  if (hasBackend()) {
    try {
      await loadActiveConnection()
    } catch {
      // keep connections screen if active session is not available
    }
  }

  render()
}

init()
