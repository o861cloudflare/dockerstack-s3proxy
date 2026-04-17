/**
 * test/cron-api.test.js
 * Builtin cron jobs should exist without saving and support protected external trigger API.
 */

import { createServer } from 'http'
import { mkdirSync, existsSync, unlinkSync } from 'fs'

process.env.PROXY_API_KEY = process.env.PROXY_API_KEY || 'test'
process.env.FIREBASE_RTDB_URL = process.env.FIREBASE_RTDB_URL || 'https://dummy.firebaseio.com'
process.env.FIREBASE_DB_SECRET = process.env.FIREBASE_DB_SECRET || 'dummy'
process.env.CRONTAB = ''
process.env.CRON_ENABLED = 'false'
process.env.LOG_LEVEL = 'fatal'
const TEST_DB_DIR = '../../.docker-volumes/s3proxy-data'
process.env.SQLITE_PATH = `${TEST_DB_DIR}/test-cron-api.db`

const TEST_DB = process.env.SQLITE_PATH
mkdirSync(TEST_DB_DIR, { recursive: true })
for (const file of [TEST_DB, `${TEST_DB}-shm`, `${TEST_DB}-wal`]) {
  if (existsSync(file)) unlinkSync(file)
}

const Fastify = (await import('fastify')).default
const authPlugin = (await import('../src/plugins/auth.js')).default
const errorHandler = (await import('../src/plugins/errorHandler.js')).default
const adminRoutes = (await import('../src/routes/admin.js')).default
const { upsertAccount } = await import('../src/db.js')
const { reloadAccountsFromSQLite } = await import('../src/accountPool.js')
const { startCronScheduler, stopCronScheduler } = await import('../src/cronScheduler.js')

let passed = 0
let failed = 0

function ok(label) {
  console.log(`✅ ${label}`)
  passed++
}

function fail(label, err) {
  console.error(`❌ ${label}`)
  console.error(`   ${err?.message || err}`)
  failed++
}

function assert(condition, message) {
  if (!condition) throw new Error(message)
}

async function readBody(req) {
  const chunks = []
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  }
  return Buffer.concat(chunks)
}

async function startFakeS3() {
  const objects = new Map()

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1')
    const parts = url.pathname.split('/').filter(Boolean)
    const bucket = parts[0] || ''
    const key = parts.slice(1).join('/')
    const objectId = `${bucket}/${key}`

    if (req.method === 'PUT') {
      const body = await readBody(req)
      objects.set(objectId, body)
      res.statusCode = 200
      res.setHeader('ETag', '"put-etag"')
      res.end('')
      return
    }

    if (req.method === 'GET' && key === '') {
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/xml')
      res.end('<?xml version="1.0" encoding="UTF-8"?><ListBucketResult></ListBucketResult>')
      return
    }

    res.statusCode = 404
    res.end('missing object')
  })

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  return {
    endpoint: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve())),
  }
}

async function createApp() {
  const fastify = Fastify({ logger: false })
  await fastify.register(authPlugin)
  await fastify.register(errorHandler)
  await fastify.register(adminRoutes)
  return fastify
}

async function main() {
  console.log('─'.repeat(60))
  console.log('T6 - Builtin Cron API Tests')
  console.log('─'.repeat(60))

  const upstream = await startFakeS3()
  const fastify = await createApp()

  try {
    upsertAccount({
      account_id: 'acc1',
      access_key_id: 'key-1',
      secret_key: 'secret-1',
      endpoint: upstream.endpoint,
      region: 'ap-southeast-1',
      bucket: 'internal-bucket',
      quota_bytes: 5_000_000_000,
      used_bytes: 0,
      active: 1,
      added_at: Date.now(),
      addressing_style: 'path',
      payload_signing_mode: 'unsigned',
    })
    reloadAccountsFromSQLite()
    await startCronScheduler({ info() {}, warn() {}, error() {} })

    try {
      const res = await fastify.inject({ method: 'GET', url: '/admin/api/overview' })
      const body = res.json()
      const jobIds = (body.jobs || []).map((job) => job.jobId)
      assert(res.statusCode === 200, `overview status=${res.statusCode}`)
      assert(jobIds.includes('probe_active_accounts'), 'missing builtin probe_active_accounts')
      assert(jobIds.includes('keepalive_touch'), 'missing builtin keepalive_touch')
      assert(jobIds.includes('keepalive_scan'), 'missing builtin keepalive_scan')
      ok('GET /admin/api/overview -> built-in cron jobs luôn có sẵn dù CRON_ENABLED=false')
    } catch (err) {
      fail('GET /admin/api/overview builtins', err)
    }

    try {
      const res = await fastify.inject({ method: 'POST', url: '/admin/api/cron-jobs/probe_active_accounts/run' })
      const body = res.json()
      assert(res.statusCode === 200, `admin run status=${res.statusCode}`)
      assert(body.ok === true, `admin run ok=${body.ok}`)
      assert(body.jobId === 'probe_active_accounts', `admin run jobId=${body.jobId}`)
      ok('POST /admin/api/cron-jobs/probe_active_accounts/run -> chạy được không cần save cron job')
    } catch (err) {
      fail('POST /admin/api/cron-jobs/probe_active_accounts/run', err)
    }

    try {
      const res = await fastify.inject({ method: 'POST', url: '/api/cron-jobs/keepalive_touch/run' })
      assert(res.statusCode === 403, `external api without auth status=${res.statusCode}`)
      ok('POST /api/cron-jobs/:jobId/run -> bị chặn nếu thiếu x-api-key')
    } catch (err) {
      fail('POST /api/cron-jobs/:jobId/run without auth', err)
    }

    try {
      const res = await fastify.inject({
        method: 'POST',
        url: '/api/cron-jobs/keepalive_touch/run',
        headers: {
          'x-api-key': 'test',
          'content-type': 'application/json',
        },
        payload: {
          payload: {
            prefix: '_external_keepalive',
            contentPrefix: 'external-cron',
          },
        },
      })
      const body = res.json()
      assert(res.statusCode === 200, `external api with auth status=${res.statusCode}`)
      assert(body.ok === true, `external api with auth ok=${body.ok}`)
      assert(body.jobId === 'keepalive_touch', `external api jobId=${body.jobId}`)
      assert(body.report?.payload?.prefix === '_external_keepalive', `external payload prefix=${body.report?.payload?.prefix}`)
      ok('POST /api/cron-jobs/keepalive_touch/run + x-api-key -> chạy được từ cron bên ngoài và nhận override payload')
    } catch (err) {
      fail('POST /api/cron-jobs/keepalive_touch/run with auth', err)
    }
  } finally {
    stopCronScheduler()
    await fastify.close().catch(() => {})
    await upstream.close().catch(() => {})
  }

  console.log('─'.repeat(60))
  console.log(`Kết quả: ${passed} passed, ${failed} failed`)
  console.log('─'.repeat(60))

  if (failed > 0) process.exit(1)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
