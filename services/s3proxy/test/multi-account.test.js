/**
 * test/multi-account.test.js
 * Multi-account routing integration tests with local fake S3 upstreams.
 */

import { createServer } from 'http'
import { mkdirSync, existsSync, unlinkSync } from 'fs'

process.env.PROXY_API_KEY = process.env.PROXY_API_KEY || 'test'
process.env.FIREBASE_RTDB_URL = process.env.FIREBASE_RTDB_URL || 'https://dummy.firebaseio.com'
process.env.FIREBASE_DB_SECRET = process.env.FIREBASE_DB_SECRET || 'dummy'
const TEST_DB_DIR = '../../.docker-volumes/s3proxy-data'
process.env.SQLITE_PATH = `${TEST_DB_DIR}/test-multi-account.db`
process.env.LOG_LEVEL = 'fatal'

const TEST_DB = process.env.SQLITE_PATH
mkdirSync(TEST_DB_DIR, { recursive: true })
for (const file of [TEST_DB, `${TEST_DB}-shm`, `${TEST_DB}-wal`]) {
  if (existsSync(file)) unlinkSync(file)
}

const Fastify = (await import('fastify')).default
const authPlugin = (await import('../src/plugins/auth.js')).default
const errorHandler = (await import('../src/plugins/errorHandler.js')).default
const healthRoutes = (await import('../src/routes/health.js')).default
const metricsRoutes = (await import('../src/routes/metrics.js')).default
const s3Routes = (await import('../src/routes/s3.js')).default
const {
  db,
  getRoute,
  ROUTE_STATE,
  setUsedBytesAbsolute,
  upsertAccount,
} = await import('../src/db.js')
const { reloadAccountsFromSQLite } = await import('../src/accountPool.js')

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
  if (!condition) {
    throw new Error(message)
  }
}

function encodedKey(bucket, objectKey) {
  return Buffer.from(`${bucket}/${objectKey}`).toString('base64url')
}

async function readBody(req) {
  const chunks = []
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  }
  return Buffer.concat(chunks)
}

async function startFakeS3(name) {
  const objects = new Map()

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1')
    const parts = url.pathname.split('/').filter(Boolean)
    const bucket = parts[0] || ''
    const key = parts.slice(1).join('/')
    const objectId = `${bucket}/${key}`

    if (req.method === 'PUT') {
      const body = await readBody(req)
      objects.set(objectId, {
        body,
        contentType: req.headers['content-type'] || 'application/octet-stream',
        lastModified: new Date().toUTCString(),
      })
      res.statusCode = 200
      res.setHeader('ETag', `"${name}-put-etag"`)
      res.end('')
      return
    }

    if (req.method === 'GET' && key === '') {
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/xml')
      res.end('<?xml version="1.0" encoding="UTF-8"?><ListBucketResult></ListBucketResult>')
      return
    }

    if (req.method === 'GET') {
      const object = objects.get(objectId)
      if (!object) {
        res.statusCode = 404
        res.end('missing object')
        return
      }
      res.statusCode = 200
      res.setHeader('Content-Type', object.contentType)
      res.setHeader('Content-Length', object.body.length)
      res.setHeader('Last-Modified', object.lastModified)
      res.setHeader('ETag', `"${name}-get-etag"`)
      res.end(object.body)
      return
    }

    if (req.method === 'HEAD') {
      const object = objects.get(objectId)
      if (!object) {
        res.statusCode = 404
        res.end('')
        return
      }
      res.statusCode = 200
      res.setHeader('Content-Type', object.contentType)
      res.setHeader('Content-Length', object.body.length)
      res.setHeader('Last-Modified', object.lastModified)
      res.setHeader('ETag', `"${name}-head-etag"`)
      res.end('')
      return
    }

    if (req.method === 'DELETE') {
      objects.delete(objectId)
      res.statusCode = 204
      res.end('')
      return
    }

    res.statusCode = 405
    res.end('method not allowed')
  })

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()

  return {
    name,
    endpoint: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve())),
    hasObject(bucket, key) {
      return objects.has(`${bucket}/${key}`)
    },
    getObjectBody(bucket, key) {
      return objects.get(`${bucket}/${key}`)?.body?.toString('utf8') ?? null
    },
  }
}

function seedAccounts(upstreamA, upstreamB) {
  upsertAccount({
    account_id: 'acc1',
    access_key_id: 'key-1',
    secret_key: 'secret-1',
    endpoint: upstreamA.endpoint,
    region: 'ap-southeast-1',
    bucket: 'acc1-physical',
    quota_bytes: 12,
    used_bytes: 0,
    active: 1,
    added_at: Date.now(),
  })
  upsertAccount({
    account_id: 'acc2',
    access_key_id: 'key-2',
    secret_key: 'secret-2',
    endpoint: upstreamB.endpoint,
    region: 'ap-southeast-1',
    bucket: 'acc2-physical',
    quota_bytes: 100,
    used_bytes: 0,
    active: 1,
    added_at: Date.now(),
  })
  reloadAccountsFromSQLite()
}

async function testCreateDeleteEmptyBucket(fastify) {
  try {
    const createRes = await fastify.inject({
      method: 'PUT',
      url: '/empty-bucket',
      headers: { 'x-api-key': 'test' },
    })
    const deleteRes = await fastify.inject({
      method: 'DELETE',
      url: '/empty-bucket',
      headers: { 'x-api-key': 'test' },
    })

    assert(createRes.statusCode === 200, `create bucket status=${createRes.statusCode}`)
    assert(deleteRes.statusCode === 204, `delete bucket status=${deleteRes.statusCode}`)
    ok('Bucket rong: PUT /:bucket -> 200 va DELETE /:bucket -> 204')
  } catch (err) {
    fail('Bucket rong create/delete', err)
  }
}

async function testRouteUploadsAcrossAccounts(fastify, upstreamA, upstreamB) {
  try {
    const createBucket = await fastify.inject({
      method: 'PUT',
      url: '/rotating-bucket',
      headers: { 'x-api-key': 'test' },
    })

    const firstPut = await fastify.inject({
      method: 'PUT',
      url: '/rotating-bucket/alpha.txt',
      headers: {
        'x-api-key': 'test',
        'content-type': 'text/plain',
      },
      payload: 'AAAA',
    })
    const secondPut = await fastify.inject({
      method: 'PUT',
      url: '/rotating-bucket/beta.txt',
      headers: {
        'x-api-key': 'test',
        'content-type': 'text/plain',
      },
      payload: 'BBBBBB',
    })

    const alphaRoute = getRoute(encodedKey('rotating-bucket', 'alpha.txt'))
    const betaRoute = getRoute(encodedKey('rotating-bucket', 'beta.txt'))

    assert(createBucket.statusCode === 200, `create rotating-bucket status=${createBucket.statusCode}`)
    assert(firstPut.statusCode === 200, `first PUT status=${firstPut.statusCode}`)
    assert(secondPut.statusCode === 200, `second PUT status=${secondPut.statusCode}`)
    assert(alphaRoute?.account_id === 'acc1', `alpha account=${alphaRoute?.account_id}`)
    assert(betaRoute?.account_id === 'acc2', `beta account=${betaRoute?.account_id}`)
    assert(upstreamA.hasObject('acc1-physical', 'rotating-bucket/alpha.txt'), 'alpha missing on acc1 upstream')
    assert(upstreamB.hasObject('acc2-physical', 'rotating-bucket/beta.txt'), 'beta missing on acc2 upstream')
    ok('Upload moi duoc phan bo sang 2 account backend khac nhau theo used_bytes')
  } catch (err) {
    fail('Upload phan bo multi-account', err)
  }
}

async function testCommonS3FlowsAcrossAccounts(fastify) {
  try {
    const getAlpha = await fastify.inject({
      method: 'GET',
      url: '/rotating-bucket/alpha.txt',
      headers: { 'x-api-key': 'test' },
    })
    const getBeta = await fastify.inject({
      method: 'GET',
      url: '/rotating-bucket/beta.txt',
      headers: { 'x-api-key': 'test' },
    })
    const headBeta = await fastify.inject({
      method: 'HEAD',
      url: '/rotating-bucket/beta.txt',
      headers: { 'x-api-key': 'test' },
    })
    const listRes = await fastify.inject({
      method: 'GET',
      url: '/rotating-bucket?list-type=2',
      headers: { 'x-api-key': 'test' },
    })
    const deleteBucketRes = await fastify.inject({
      method: 'DELETE',
      url: '/rotating-bucket',
      headers: { 'x-api-key': 'test' },
    })

    assert(getAlpha.statusCode === 200, `GET alpha status=${getAlpha.statusCode}`)
    assert(getAlpha.payload === 'AAAA', `GET alpha payload=${getAlpha.payload}`)
    assert(getBeta.statusCode === 200, `GET beta status=${getBeta.statusCode}`)
    assert(getBeta.payload === 'BBBBBB', `GET beta payload=${getBeta.payload}`)
    assert(headBeta.statusCode === 200, `HEAD beta status=${headBeta.statusCode}`)
    assert(headBeta.payload === '', `HEAD beta payload length=${headBeta.payload.length}`)
    assert(listRes.statusCode === 200, `LIST status=${listRes.statusCode}`)
    assert(listRes.payload.includes('<Key>alpha.txt</Key>'), 'list missing alpha')
    assert(listRes.payload.includes('<Key>beta.txt</Key>'), 'list missing beta')
    assert(deleteBucketRes.statusCode === 409, `delete non-empty bucket status=${deleteBucketRes.statusCode}`)
    assert(deleteBucketRes.payload.includes('<Code>BucketNotEmpty</Code>'), 'missing BucketNotEmpty XML')
    ok('Luong S3 thuong dung: GET, HEAD, LIST, DELETE bucket khong rong deu dung tren multi-account')
  } catch (err) {
    fail('Luong S3 thuong dung tren multi-account', err)
  }
}

async function testDeleteObjectsAndLogicalBucket(fastify, upstreamA, upstreamB) {
  try {
    const deleteAlpha = await fastify.inject({
      method: 'DELETE',
      url: '/rotating-bucket/alpha.txt',
      headers: { 'x-api-key': 'test' },
    })
    const deleteBeta = await fastify.inject({
      method: 'DELETE',
      url: '/rotating-bucket/beta.txt',
      headers: { 'x-api-key': 'test' },
    })
    const alphaRoute = getRoute(encodedKey('rotating-bucket', 'alpha.txt'))
    const betaRoute = getRoute(encodedKey('rotating-bucket', 'beta.txt'))
    const getDeleted = await fastify.inject({
      method: 'GET',
      url: '/rotating-bucket/alpha.txt',
      headers: { 'x-api-key': 'test' },
    })
    const listAfterDelete = await fastify.inject({
      method: 'GET',
      url: '/rotating-bucket?list-type=2',
      headers: { 'x-api-key': 'test' },
    })
    const deleteBucketRes = await fastify.inject({
      method: 'DELETE',
      url: '/rotating-bucket',
      headers: { 'x-api-key': 'test' },
    })

    assert(deleteAlpha.statusCode === 204, `DELETE alpha status=${deleteAlpha.statusCode}`)
    assert(deleteBeta.statusCode === 204, `DELETE beta status=${deleteBeta.statusCode}`)
    assert(alphaRoute?.state === ROUTE_STATE.DELETED, `alpha route state=${alphaRoute?.state}`)
    assert(betaRoute?.state === ROUTE_STATE.DELETED, `beta route state=${betaRoute?.state}`)
    assert(!upstreamA.hasObject('acc1-physical', 'rotating-bucket/alpha.txt'), 'alpha still exists on acc1 upstream')
    assert(!upstreamB.hasObject('acc2-physical', 'rotating-bucket/beta.txt'), 'beta still exists on acc2 upstream')
    assert(getDeleted.statusCode === 404, `GET deleted alpha status=${getDeleted.statusCode}`)
    assert(!listAfterDelete.payload.includes('<Key>alpha.txt</Key>'), 'deleted alpha still listed')
    assert(!listAfterDelete.payload.includes('<Key>beta.txt</Key>'), 'deleted beta still listed')
    assert(deleteBucketRes.statusCode === 204, `delete empty logical bucket status=${deleteBucketRes.statusCode}`)
    ok('DELETE object giu tombstone, an khoi LIST va cho phep xoa bucket sau khi rong')
  } catch (err) {
    fail('DELETE object + xoa bucket logic', err)
  }
}

async function testThresholdSwitchAndOverwrite(fastify, upstreamA, upstreamB) {
  try {
    const createBucket = await fastify.inject({
      method: 'PUT',
      url: '/threshold-bucket',
      headers: { 'x-api-key': 'test' },
    })

    setUsedBytesAbsolute('acc1', 10)
    setUsedBytesAbsolute('acc2', 11)
    reloadAccountsFromSQLite()

    const thresholdPut = await fastify.inject({
      method: 'PUT',
      url: '/threshold-bucket/gamma.txt',
      headers: {
        'x-api-key': 'test',
        'content-type': 'text/plain',
      },
      payload: 'Z',
    })
    const firstRoute = getRoute(encodedKey('threshold-bucket', 'gamma.txt'))
    const firstGet = await fastify.inject({
      method: 'GET',
      url: '/threshold-bucket/gamma.txt',
      headers: { 'x-api-key': 'test' },
    })

    assert(createBucket.statusCode === 200, `create threshold-bucket status=${createBucket.statusCode}`)
    assert(thresholdPut.statusCode === 200, `threshold PUT status=${thresholdPut.statusCode}`)
    assert(firstRoute?.account_id === 'acc2', `threshold route account=${firstRoute?.account_id}`)
    assert(!upstreamA.hasObject('acc1-physical', 'threshold-bucket/gamma.txt'), 'gamma should not be on acc1 upstream')
    assert(upstreamB.hasObject('acc2-physical', 'threshold-bucket/gamma.txt'), 'gamma missing on acc2 upstream')
    assert(firstGet.statusCode === 200, `GET gamma status=${firstGet.statusCode}`)
    assert(firstGet.payload === 'Z', `GET gamma payload=${firstGet.payload}`)

    setUsedBytesAbsolute('acc1', 0)
    setUsedBytesAbsolute('acc2', 50)
    reloadAccountsFromSQLite()

    const overwritePut = await fastify.inject({
      method: 'PUT',
      url: '/threshold-bucket/gamma.txt',
      headers: {
        'x-api-key': 'test',
        'content-type': 'text/plain',
      },
      payload: 'ZZ',
    })
    const overwrittenRoute = getRoute(encodedKey('threshold-bucket', 'gamma.txt'))
    const overwrittenGet = await fastify.inject({
      method: 'GET',
      url: '/threshold-bucket/gamma.txt',
      headers: { 'x-api-key': 'test' },
    })

    assert(overwritePut.statusCode === 200, `overwrite PUT status=${overwritePut.statusCode}`)
    assert(overwrittenRoute?.account_id === 'acc2', `overwrite route account=${overwrittenRoute?.account_id}`)
    assert(upstreamB.getObjectBody('acc2-physical', 'threshold-bucket/gamma.txt') === 'ZZ', 'gamma body not updated on acc2 upstream')
    assert(!upstreamA.hasObject('acc1-physical', 'threshold-bucket/gamma.txt'), 'gamma unexpectedly moved to acc1 upstream')
    assert(overwrittenGet.statusCode === 200, `GET overwritten gamma status=${overwrittenGet.statusCode}`)
    assert(overwrittenGet.payload === 'ZZ', `GET overwritten gamma payload=${overwrittenGet.payload}`)
    ok('Khi account sap day proxy chuyen sang account khac, va overwrite van giu account da co metadata')
  } catch (err) {
    fail('Threshold switch + overwrite', err)
  }
}

async function main() {
  console.log('─'.repeat(60))
  console.log('T6 - Multi Account Routing Tests')
  console.log('─'.repeat(60))

  const upstreamA = await startFakeS3('acc1')
  const upstreamB = await startFakeS3('acc2')
  const fastify = Fastify({ logger: false })

  try {
    seedAccounts(upstreamA, upstreamB)

    fastify.decorate('config', { INSTANCE_ID: 'test-multi-account' })
    await fastify.register(authPlugin)
    await fastify.register(errorHandler)
    await fastify.register(healthRoutes)
    await fastify.register(metricsRoutes)
    await fastify.register(s3Routes, { prefix: '/' })

    await testCreateDeleteEmptyBucket(fastify)
    await testRouteUploadsAcrossAccounts(fastify, upstreamA, upstreamB)
    await testCommonS3FlowsAcrossAccounts(fastify)
    await testDeleteObjectsAndLogicalBucket(fastify, upstreamA, upstreamB)
    await testThresholdSwitchAndOverwrite(fastify, upstreamA, upstreamB)
  } finally {
    await fastify.close().catch(() => {})
    await upstreamA.close().catch(() => {})
    await upstreamB.close().catch(() => {})
    db.close()
    for (const file of [TEST_DB, `${TEST_DB}-shm`, `${TEST_DB}-wal`]) {
      if (existsSync(file)) unlinkSync(file)
    }
  }

  console.log('─'.repeat(60))
  console.log(`Results: ${passed} passed, ${failed} failed`)
  process.exit(failed > 0 ? 1 : 0)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
