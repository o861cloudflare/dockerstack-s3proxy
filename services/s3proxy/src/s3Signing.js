/**
 * src/s3Signing.js
 * Normalize provider-specific SigV4 signing behavior.
 */

function normalizeString(value) {
  if (value === undefined || value === null) return ''
  return String(value).trim()
}

export function isSupabaseS3Endpoint(endpoint) {
  const raw = normalizeString(endpoint)
  if (!raw) return false

  try {
    const parsed = new URL(raw)
    const host = parsed.hostname.toLowerCase()
    const pathname = parsed.pathname.replace(/\/+$/, '')
    const isSupabaseHost = host.endsWith('.storage.supabase.co') || host.endsWith('.supabase.co')
    const isS3CompatPath = pathname.endsWith('/storage/v1/s3')
    return isSupabaseHost && isS3CompatPath
  } catch {
    return false
  }
}

export function resolveS3SigningRegion({ endpoint, region }) {
  const normalizedRegion = normalizeString(region).toLowerCase()
  if (!isSupabaseS3Endpoint(endpoint)) {
    return normalizedRegion || 'auto'
  }

  if (normalizedRegion === 'us-east-1' || normalizedRegion === 'auto') {
    return normalizedRegion
  }

  // Supabase S3 compatibility currently validates SigV4 in us-east-1 scope.
  return 'us-east-1'
}

