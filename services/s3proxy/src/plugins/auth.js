/**
 * src/plugins/auth.js
 * Fastify plugin: validate x-api-key / bearer auth for all protected routes.
 *
 * Security note:
 * - By default, SigV4/presigned requests are NOT authenticated by credential
 *   extraction only, because that bypasses signature/expiry verification.
 * - Legacy behavior can be toggled via ALLOW_INSECURE_SIGV4_KEY_EXTRACT=true.
 */

import fp from 'fastify-plugin'
import config from '../config.js'
import { buildErrorXml } from '../utils/s3Xml.js'

function extractHeaderApiKey(request) {
  const xApiKey = request.headers['x-api-key']
  if (xApiKey) return xApiKey.trim()

  const authHeader = request.headers['authorization']
  if (authHeader) {
    if (authHeader.toLowerCase().startsWith('bearer ')) {
      return authHeader.slice(7).trim()
    }
  }

  return null
}

function extractSigV4CredentialFromAuthHeader(request) {
  const authHeader = request.headers['authorization']
  if (!authHeader || !authHeader.startsWith('AWS4-HMAC-SHA256')) return null
  const credentialMatch = authHeader.match(/Credential=([^/,\s]+)/)
  if (!credentialMatch) return null
  return credentialMatch[1].trim()
}

function extractSigV4CredentialFromQuery(request) {
  const query = request.query
  if (!query || typeof query !== 'object') return null

  for (const [key, value] of Object.entries(query)) {
    if (typeof key !== 'string' || key.toLowerCase() !== 'x-amz-credential') continue

    const rawValue = Array.isArray(value) ? value[0] : value
    if (typeof rawValue !== 'string' || rawValue.trim() === '') continue

    const decoded = decodeURIComponent(rawValue)
    const accessKey = decoded.split('/')[0]?.trim()
    if (accessKey) return accessKey
  }

  return null
}

function hasSigV4Context(request) {
  return Boolean(extractSigV4CredentialFromAuthHeader(request))
    || Boolean(extractSigV4CredentialFromQuery(request))
}

async function authPlugin(fastify, _opts) {
  fastify.decorate('authenticate', async function (request, reply) {
    // Skip auth for routes that opt out
    if (request.routeOptions?.config?.skipAuth) return

    const reqId = request.id ?? ''
    const headerKey = extractHeaderApiKey(request)
    if (headerKey === config.PROXY_API_KEY) return

    if (config.ALLOW_INSECURE_SIGV4_KEY_EXTRACT) {
      const sigV4Key = extractSigV4CredentialFromAuthHeader(request)
        || extractSigV4CredentialFromQuery(request)
      if (sigV4Key === config.PROXY_API_KEY) return
    }

    if (hasSigV4Context(request) && !config.ALLOW_INSECURE_SIGV4_KEY_EXTRACT) {
      reply
        .code(403)
        .header('Content-Type', 'application/xml')
        .send(buildErrorXml(
          'AccessDenied',
          'SigV4/presigned credential extraction is disabled. Provide x-api-key or Bearer auth.',
          reqId,
        ))
      return
    }

    reply
      .code(403)
      .header('Content-Type', 'application/xml')
      .send(buildErrorXml('AccessDenied', 'Access Denied', reqId))
  })
}

export default fp(authPlugin, { name: 'auth' })
