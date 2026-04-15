/**
 * src/routes/admin.js
 * Admin UI + APIs for runtime status, cron management and S3 tests.
 */

import { randomBytes } from 'crypto'
import { readFileSync } from 'fs'
import { dirname, join } from 'path'
import { fileURLToPath } from 'url'
import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadBucketCommand,
  ListObjectsV2Command,
  PutObjectCommand,
} from '@aws-sdk/client-s3'

import config from '../config.js'
import {
  deleteAccount,
  getAccountById,
  getAllAccounts,
  getTrackedRoutesByAccount,
  upsertAccount,
} from '../db.js'
import { getAccountsStats, reloadAccountsFromRTDB, reloadAccountsFromSQLite } from '../accountPool.js'
import { rtdbBatchPatch } from '../firebase.js'
import { buildRtdbAccountPath, suggestAccountId, validateAccountIdForRealtime } from '../accountId.js'
import { getRtdbState } from './health.js'
import {
  getCronJobKinds,
  listCronJobs,
  removeCronJob,
  runCronJobNow,
  saveCronJob,
} from '../cronScheduler.js'
import { createS3Client } from '../inventoryScanner.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const adminHtml = readFileSync(join(__dirname, '..', 'admin-ui.html'), 'utf-8')
const adminIcon = readFileSync(join(__dirname, '..', 'admin-icon.svg'), 'utf-8')
const DEFAULT_ADMIN_QUOTA_BYTES = 1024 * 1024 * 1024

const adminServiceWorker = `
const CACHE_NAME = 's3proxy-admin-v1'
const ADMIN_SHELL = ['/admin', '/admin/manifest.webmanifest', '/admin/icon.svg']

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(ADMIN_SHELL)).then(() => self.skipWaiting()),
  )
})

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim()),
  )
})

self.addEventListener('fetch', (event) => {
  const request = event.request
  if (request.method !== 'GET') return

  const url = new URL(request.url)
  if (!url.pathname.startsWith('/admin') || url.pathname.startsWith('/admin/api/')) return

  event.respondWith(
    fetch(request)
      .then((response) => {
        const clone = response.clone()
        caches.open(CACHE_NAME).then((cache) => cache.put(request, clone)).catch(() => {})
        return response
      })
      .catch(() => caches.match(request).then((cached) => cached || caches.match('/admin'))),
  )
})
`.trim()

function formatPercent(used, quota) {
  if (!quota) return 0
  return Number(((used / quota) * 100).toFixed(2))
}

function toPublicAccount(row) {
  return {
    accountId: row.account_id,
    accessKeyId: row.access_key_id,
    endpoint: row.endpoint,
    region: row.region,
    bucket: row.bucket,
    addressingStyle: row.addressing_style ?? 'path',
    payloadSigningMode: row.payload_signing_mode ?? 'unsigned',
    active: row.active === 1 || row.active === true,
    usedBytes: row.used_bytes ?? 0,
    quotaBytes: row.quota_bytes ?? 0,
    usedPercent: formatPercent(row.used_bytes ?? 0, row.quota_bytes ?? 0),
    addedAt: row.added_at ?? null,
    hasSecret: Boolean(row.secret_key),
  }
}

function normalizeString(value) {
  if (value === undefined || value === null) return ''
  return String(value).trim()
}

function normalizeBoolean(value, fallback = true) {
  if (value === undefined || value === null || value === '') return fallback
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0

  const normalized = String(value).trim().toLowerCase()
  if (['true', '1', 'yes', 'y', 'on'].includes(normalized)) return true
  if (['false', '0', 'no', 'n', 'off'].includes(normalized)) return false
  return fallback
}

function normalizeAddressingStyle(value) {
  const raw = normalizeString(value).toLowerCase()
  if (!raw) return 'path'
  if (['path', 'path-style', 'path_style'].includes(raw)) return 'path'
  if (['virtual', 'virtual-hosted', 'virtual_hosted', 'virtual-hosted-style'].includes(raw)) return 'virtual'
  return ''
}

function normalizePayloadSigningMode(value) {
  const raw = normalizeString(value).toLowerCase()
  if (!raw) return 'unsigned'
  if (['unsigned', 'unsigned-payload', 'unsigned_payload'].includes(raw)) return 'unsigned'
  if (['signed', 'strict', 'required'].includes(raw)) return 'signed'
  return ''
}

function normalizePositiveInteger(value, fallback, fieldName, errors) {
  if (value === undefined || value === null || value === '') return fallback
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric <= 0) {
    errors.push(`${fieldName} must be a positive number`)
    return fallback
  }
  return Math.trunc(numeric)
}

function normalizeNonNegativeInteger(value, fallback, fieldName, errors) {
  if (value === undefined || value === null || value === '') return fallback
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric < 0) {
    errors.push(`${fieldName} must be a non-negative number`)
    return fallback
  }
  return Math.trunc(numeric)
}

function toRtdbAccountDocument(account) {
  return {
    accountId: account.account_id,
    accessKeyId: account.access_key_id,
    secretAccessKey: account.secret_key,
    endpoint: account.endpoint,
    region: account.region,
    bucket: account.bucket,
    addressingStyle: account.addressing_style ?? 'path',
    payloadSigningMode: account.payload_signing_mode ?? 'unsigned',
    quotaBytes: account.quota_bytes,
    usedBytes: account.used_bytes,
    active: account.active === 1,
    addedAt: account.added_at,
  }
}

function normalizeAccountPayload(payload, existing = null) {
  const errors = []

  const accountIdInput = normalizeString(payload.accountId ?? payload.account_id ?? existing?.account_id)
  const accountIdValidation = validateAccountIdForRealtime(accountIdInput)
  if (!accountIdValidation.valid) {
    let message = `accountId ${accountIdValidation.reason}`
    const suggestion = suggestAccountId(accountIdInput)
    if (suggestion && suggestion !== accountIdValidation.accountId) {
      message += ` (suggested: ${suggestion})`
    }
    errors.push(message)
  }

  const accessKeyId = normalizeString(payload.accessKeyId ?? payload.access_key_id ?? existing?.access_key_id)
  const secretKey = normalizeString(payload.secretAccessKey ?? payload.secret_key) || existing?.secret_key || ''
  const endpoint = normalizeString(payload.endpoint ?? existing?.endpoint)
  const region = normalizeString(payload.region ?? existing?.region)
  const bucket = normalizeString(payload.bucket ?? existing?.bucket)
  const addressingStyle = normalizeAddressingStyle(payload.addressingStyle ?? payload.addressing_style ?? existing?.addressing_style)
  const payloadSigningMode = normalizePayloadSigningMode(
    payload.payloadSigningMode ?? payload.payload_signing_mode ?? existing?.payload_signing_mode,
  )
  const quotaBytes = normalizePositiveInteger(
    payload.quotaBytes ?? payload.quota_bytes,
    existing?.quota_bytes ?? DEFAULT_ADMIN_QUOTA_BYTES,
    'quotaBytes',
    errors,
  )
  const usedBytes = normalizeNonNegativeInteger(
    payload.usedBytes ?? payload.used_bytes,
    existing?.used_bytes ?? 0,
    'usedBytes',
    errors,
  )
  const addedAt = normalizeNonNegativeInteger(
    payload.addedAt ?? payload.added_at,
    existing?.added_at ?? Date.now(),
    'addedAt',
    errors,
  )
  const active = normalizeBoolean(payload.active, existing ? (existing.active === 1 || existing.active === true) : true) ? 1 : 0

  if (!accessKeyId) errors.push('accessKeyId is required')
  if (!secretKey) errors.push('secretAccessKey is required')
  if (!endpoint) errors.push('endpoint is required')
  if (!region) errors.push('region is required')
  if (!bucket) errors.push('bucket is required')
  if (!addressingStyle) errors.push('addressingStyle must be one of: path, virtual')
  if (!payloadSigningMode) errors.push('payloadSigningMode must be one of: unsigned, signed')

  if (endpoint) {
    try {
      const parsed = new URL(endpoint)
      if (!['http:', 'https:'].includes(parsed.protocol)) {
        errors.push('endpoint must use http or https')
      }
    } catch {
      errors.push('endpoint must be a valid URL')
    }
  }

  if (errors.length > 0) {
    return { errors, row: null, accountId: accountIdValidation.accountId || accountIdInput }
  }

  return {
    errors,
    accountId: accountIdValidation.accountId,
    row: {
      account_id: accountIdValidation.accountId,
      access_key_id: accessKeyId,
      secret_key: secretKey,
      endpoint,
      region,
      bucket,
      addressing_style: addressingStyle,
      payload_signing_mode: payloadSigningMode,
      quota_bytes: quotaBytes,
      used_bytes: usedBytes,
      active,
      added_at: addedAt,
    },
  }
}

async function assertBucketExists(accountRow) {
  const client = createS3Client(accountRow)
  await client.send(new HeadBucketCommand({ Bucket: accountRow.bucket }))
}

function pocketbaseCompatibility() {
  return {
    supported: {
      putObject: true,
      getObject: true,
      deleteObject: true,
      headObject: true,
      multipartUpload: true,
      listBucket: true,
      presignedStyleAuth: true,
    },
    caveats: [
      'Nên chạy PocketBase với S3 path-style endpoint trỏ thẳng vào s3proxy.',
      'Admin endpoint /admin hiện skip x-api-key, cần đặt sau Caddy Basic Auth.',
      'Một số API S3 nâng cao (ACL/policy/lifecycle...) chưa implement đầy đủ.',
    ],
  }
}

function readStreamBodyToString(body) {
  if (!body) return Promise.resolve('')
  if (typeof body.transformToString === 'function') {
    return body.transformToString()
  }

  return new Promise((resolve, reject) => {
    const chunks = []
    body.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
    body.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')))
    body.on('error', reject)
  })
}

async function runS3Probe(account) {
  const client = createS3Client(account)
  const probeKey = `${config.ADMIN_TEST_PREFIX}/${account.account_id}-${Date.now()}-${randomBytes(3).toString('hex')}.txt`
  const payload = `s3proxy probe ${new Date().toISOString()}`

  const timings = {}
  const startedAt = Date.now()

  const t1 = Date.now()
  await client.send(new ListObjectsV2Command({ Bucket: account.bucket, MaxKeys: 3 }))
  timings.listMs = Date.now() - t1

  const t2 = Date.now()
  await client.send(new PutObjectCommand({
    Bucket: account.bucket,
    Key: probeKey,
    Body: payload,
    ContentType: 'text/plain; charset=utf-8',
  }))
  timings.putMs = Date.now() - t2

  const t3 = Date.now()
  const getResult = await client.send(new GetObjectCommand({ Bucket: account.bucket, Key: probeKey }))
  const fetchedPayload = await readStreamBodyToString(getResult.Body)
  timings.getMs = Date.now() - t3

  const t4 = Date.now()
  await client.send(new DeleteObjectCommand({ Bucket: account.bucket, Key: probeKey }))
  timings.deleteMs = Date.now() - t4

  return {
    accountId: account.account_id,
    bucket: account.bucket,
    probeKey,
    ok: fetchedPayload === payload,
    bytes: payload.length,
    durationMs: Date.now() - startedAt,
    timings,
  }
}

function parseBodyObject(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return {}
  return body
}

export default async function adminRoutes(fastify, _opts) {
  fastify.get('/admin', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    reply.type('text/html; charset=utf-8').send(adminHtml)
  })

  fastify.get('/admin/icon.svg', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    reply.type('image/svg+xml; charset=utf-8').send(adminIcon)
  })

  fastify.get('/admin/manifest.webmanifest', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    reply.type('application/manifest+json').send({
      name: 'S3Proxy Admin',
      short_name: 'S3Proxy',
      description: 'Admin console for S3Proxy accounts, probes and cron jobs',
      start_url: '/admin',
      scope: '/admin/',
      display: 'standalone',
      background_color: '#0b1020',
      theme_color: '#0b1020',
      icons: [{
        src: '/admin/icon.svg',
        sizes: 'any',
        type: 'image/svg+xml',
        purpose: 'any maskable',
      }],
    })
  })

  fastify.get('/admin/sw.js', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    reply.type('application/javascript; charset=utf-8').send(adminServiceWorker)
  })

  fastify.get('/admin/api/overview', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    const stats = getAccountsStats()
    const accounts = getAllAccounts().map(toPublicAccount)
    const rtdb = getRtdbState()

    reply.send({
      status: rtdb.connected ? 'ok' : 'degraded',
      instanceId: config.INSTANCE_ID,
      deployVersion: config.DEPLOY_VERSION,
      stats,
      rtdb,
      jobs: listCronJobs(),
      cronKinds: getCronJobKinds(),
      accounts,
      compatibility: pocketbaseCompatibility(),
    })
  })

  fastify.get('/admin/api/accounts', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    const accounts = getAllAccounts().map(toPublicAccount)
    return reply.send({
      total: accounts.length,
      accounts,
    })
  })

  fastify.post('/admin/api/accounts', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const payload = parseBodyObject(request.body)
    const requestedId = normalizeString(payload.accountId ?? payload.account_id)
    const existing = requestedId ? getAccountById(requestedId) : null
    const normalized = normalizeAccountPayload(payload, existing)

    if (normalized.errors.length > 0 || !normalized.row) {
      return reply.code(400).send({
        ok: false,
        error: 'Invalid account payload',
        errors: normalized.errors,
      })
    }

    try {
      await assertBucketExists(normalized.row)
    } catch (err) {
      const message = err?.message ?? String(err)
      return reply.code(400).send({
        ok: false,
        error: `Bucket "${normalized.row.bucket}" does not exist or is not accessible`,
        detail: message,
      })
    }

    const beforeUpsert = getAccountById(normalized.row.account_id)
    upsertAccount(normalized.row)
    reloadAccountsFromSQLite()

    const updates = {
      [buildRtdbAccountPath(normalized.row.account_id)]: toRtdbAccountDocument(normalized.row),
    }

    let rtdbSynced = true
    let warning = ''
    try {
      await rtdbBatchPatch(updates)
      await reloadAccountsFromRTDB()
    } catch (err) {
      rtdbSynced = false
      warning = `Account saved locally, but RTDB sync failed: ${err?.message ?? String(err)}`
      request.log.warn({ err, accountId: normalized.row.account_id }, 'admin account sync failed')
      reloadAccountsFromSQLite()
    }

    return reply.send({
      ok: true,
      action: beforeUpsert ? 'updated' : 'created',
      rtdbSynced,
      warning: warning || undefined,
      account: toPublicAccount(normalized.row),
    })
  })

  fastify.delete('/admin/api/accounts/:accountId', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const accountId = normalizeString(request.params?.accountId)
    if (!accountId) {
      return reply.code(400).send({ ok: false, error: 'accountId is required' })
    }

    const existing = getAccountById(accountId)
    if (!existing) {
      return reply.code(404).send({ ok: false, error: 'account not found' })
    }

    const trackedRoutes = getTrackedRoutesByAccount(accountId)
    if (trackedRoutes.length > 0) {
      return reply.code(409).send({
        ok: false,
        error: `account has ${trackedRoutes.length} tracked route(s), cannot delete`,
      })
    }

    try {
      deleteAccount(accountId)
    } catch (err) {
      const message = err?.message ?? String(err)
      if (message.includes('FOREIGN KEY')) {
        return reply.code(409).send({
          ok: false,
          error: 'account still referenced by object metadata, cannot delete',
        })
      }
      throw err
    }
    reloadAccountsFromSQLite()

    let rtdbSynced = true
    let warning = ''
    try {
      await rtdbBatchPatch({
        [buildRtdbAccountPath(accountId)]: null,
      })
      await reloadAccountsFromRTDB()
    } catch (err) {
      rtdbSynced = false
      warning = `Account deleted locally, but RTDB sync failed: ${err?.message ?? String(err)}`
      request.log.warn({ err, accountId }, 'admin account delete sync failed')
      reloadAccountsFromSQLite()
    }

    return reply.send({
      ok: true,
      accountId,
      rtdbSynced,
      warning: warning || undefined,
    })
  })

  fastify.post('/admin/api/cron-jobs', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    try {
      const saved = saveCronJob(parseBodyObject(request.body))
      return reply.send({ ok: true, job: saved })
    } catch (err) {
      return reply.code(400).send({ ok: false, error: err?.message ?? String(err) })
    }
  })

  fastify.post('/admin/api/cron-jobs/:jobId/run', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    try {
      const result = await runCronJobNow(request.params.jobId)
      return reply.send({ ok: true, jobId: result.job_id, lastRunStatus: result.lastRunStatus })
    } catch (err) {
      return reply.code(404).send({ ok: false, error: err?.message ?? String(err) })
    }
  })

  fastify.delete('/admin/api/cron-jobs/:jobId', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    try {
      const removed = removeCronJob(request.params.jobId)
      if (!removed) {
        return reply.code(404).send({ ok: false, error: 'job not found' })
      }
      return reply.send({ ok: true })
    } catch (err) {
      return reply.code(400).send({ ok: false, error: err?.message ?? String(err) })
    }
  })

  fastify.post('/admin/api/test-s3', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const payload = parseBodyObject(request.body)
    const all = getAllAccounts().filter((item) => item.active === 1 || item.active === true)

    let targets = []
    if (payload.allActive === true) {
      targets = all
    } else if (payload.accountId) {
      targets = all.filter((item) => item.account_id === String(payload.accountId))
    }

    if (targets.length === 0) {
      return reply.code(400).send({ ok: false, error: 'account not found or inactive' })
    }

    const results = []
    for (const account of targets) {
      try {
        const result = await runS3Probe(account)
        results.push(result)
      } catch (err) {
        results.push({
          accountId: account.account_id,
          bucket: account.bucket,
          ok: false,
          error: err?.message ?? String(err),
        })
      }
    }

    return reply.send({
      ok: results.every((item) => item.ok),
      count: results.length,
      results,
    })
  })
}
