/**
 * src/supabaseS3.js
 * Parse Supabase S3 text blobs and optionally enrich them via Supabase Management API.
 */

import { resolveS3SigningRegion } from './s3Signing.js'

const SUPABASE_MANAGEMENT_BASE_URL = 'https://api.supabase.com/v1'
const SUPABASE_ACCESS_TOKEN_REGEX = /^sbp_(?:[a-z0-9]+_)?[a-z0-9]{20,}$/i
const SUPABASE_ACCESS_TOKEN_SCAN_REGEX = /\bsbp_(?:[a-z0-9]+_)?[a-z0-9]{20,}\b/gi
const SUPABASE_S3_ENDPOINT_REGEX = /^https?:\/\/[a-z0-9-]+(?:\.storage\.supabase\.co|\.supabase\.co)\/storage\/v1\/s3\/?$/i
const ACCESS_KEY_ID_REGEX = /^[A-Za-z0-9][A-Za-z0-9_-]{7,127}$/
const SECRET_ACCESS_KEY_REGEX = /^[A-Za-z0-9][A-Za-z0-9/+_=.-]{15,255}$/
const EMAIL_REGEX = /^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$/

function normalizeString(value) {
  if (value === undefined || value === null) return ''
  return String(value).trim()
}

function unescapeQuotedText(value) {
  const text = normalizeString(value)
  if (!text) return ''

  try {
    return JSON.parse(`"${text.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`)
  } catch {
    return text
      .replace(/\\"/g, '"')
      .replace(/\\n/g, '\n')
      .replace(/\\r/g, '\r')
      .replace(/\\t/g, '\t')
      .trim()
  }
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function parseKeyValueObjects(rawInput) {
  const map = new Map()
  const pattern = /(?:"|')key(?:"|')\s*:\s*(?<q1>"|')(?<key>(?:\\.|(?!\k<q1>).)*)\k<q1>\s*,\s*(?:"|')value(?:"|')\s*:\s*(?<q2>"|')(?<value>(?:\\.|(?!\k<q2>).)*)\k<q2>/gms

  for (const match of rawInput.matchAll(pattern)) {
    const key = normalizeString(unescapeQuotedText(match.groups?.key))
    const value = normalizeString(unescapeQuotedText(match.groups?.value))
    if (!key) continue
    map.set(key.toLowerCase(), value)
  }

  return map
}

function extractValueByKeys(rawInput, keys = []) {
  for (const key of keys) {
    const escapedKey = escapeRegex(key)
    const quotedPattern = new RegExp(`(["'])${escapedKey}\\1\\s*[:=]\\s*(["'])(?<value>(?:\\\\.|(?!\\2).)*)\\2`, 'is')
    const quotedMatch = rawInput.match(quotedPattern)
    if (quotedMatch?.groups?.value) {
      return normalizeString(unescapeQuotedText(quotedMatch.groups.value))
    }

    const unquotedPattern = new RegExp(`(["'])?${escapedKey}\\1?\\s*[:=]\\s*([^\\r\\n,}\\]]+)`, 'i')
    const unquotedMatch = rawInput.match(unquotedPattern)
    if (unquotedMatch?.[2]) {
      return normalizeString(unquotedMatch[2].replace(/^["']|["']$/g, ''))
    }
  }

  return ''
}

function parseDatabaseDescriptor(value) {
  const result = {
    endpoint: '',
    accessKeyId: '',
    secretAccessKey: '',
    bucketName: '',
    region: '',
  }

  const descriptor = normalizeString(value)
  if (!descriptor) return result

  for (const chunk of descriptor.split('|')) {
    const match = chunk.match(/^\s*([A-Za-z][A-Za-z0-9_.-]*)\s*:\s*(.+?)\s*$/)
    if (!match) continue

    const key = match[1].toLowerCase()
    const fieldValue = normalizeString(match[2].replace(/^["']|["']$/g, ''))

    if (key === 's3url') result.endpoint = fieldValue
    if (key === 'accesskeyid') result.accessKeyId = fieldValue
    if (key === 'secretaccesskey') result.secretAccessKey = fieldValue
    if (key === 'bucket' || key === 'bucketname') result.bucketName = fieldValue
    if (key === 'region') result.region = fieldValue
  }

  return result
}

function sanitizeAccountPart(value) {
  return normalizeString(value)
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/[-_]{2,}/g, '-')
    .replace(/^[-_]+|[-_]+$/g, '')
    .slice(0, 80)
}

function sanitizeBucketName(value) {
  const normalized = normalizeString(value)
    .toLowerCase()
    .replace(/[^a-z0-9.-]+/g, '-')
    .replace(/[.-]{2,}/g, '-')
    .replace(/^[.-]+|[.-]+$/g, '')
    .slice(0, 63)

  if (!normalized) return ''
  if (normalized.length < 3) return ''
  if (!/^[a-z0-9]/.test(normalized) || !/[a-z0-9]$/.test(normalized)) return ''
  return normalized
}

function deriveBucketFromEmail(emailOwner) {
  const email = normalizeString(emailOwner).toLowerCase()
  if (!EMAIL_REGEX.test(email)) return ''

  const localPart = email.split('@')[0] || ''
  let bucket = sanitizeBucketName(localPart)
  if (!bucket) {
    bucket = sanitizeBucketName(`bucket-${localPart}`) || 'bucket-default'
  }

  return bucket
}

function deriveAccountId(emailOwner, projectRef) {
  const username = sanitizeAccountPart(normalizeString(emailOwner).split('@')[0])
  const ref = sanitizeAccountPart(projectRef)

  if (username && ref) return `${username}-${ref}`.slice(0, 80)
  if (username) return username.slice(0, 80)
  if (ref) return `supabase-${ref}`.slice(0, 80)
  return 'supabase-account'
}

function deriveProjectRefFromEndpoint(endpoint) {
  const raw = normalizeString(endpoint)
  if (!raw) return ''

  try {
    const parsed = new URL(raw)
    const host = parsed.hostname.toLowerCase()
    const storageMatch = host.match(/^([a-z0-9-]+)\.storage\.supabase\.co$/)
    if (storageMatch?.[1]) return storageMatch[1]
    const legacyMatch = host.match(/^([a-z0-9-]+)\.supabase\.co$/)
    if (legacyMatch?.[1]) return legacyMatch[1]
  } catch {
    return ''
  }

  return ''
}

function normalizeEndpoint(value) {
  const raw = normalizeString(value)
  if (!raw) return ''

  try {
    const parsed = new URL(raw)
    if (!['http:', 'https:'].includes(parsed.protocol)) return ''
    return parsed.toString().replace(/\/$/, '')
  } catch {
    return ''
  }
}

function extractFirstEmail(rawInput) {
  const match = rawInput.match(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/)
  return normalizeString(match?.[0])
}

function extractFirstAccessToken(rawInput) {
  const match = rawInput.match(SUPABASE_ACCESS_TOKEN_SCAN_REGEX)
  return normalizeString(match?.[0] || '')
}

export function isSupabaseAccessToken(value) {
  const token = normalizeString(value)
  return Boolean(token) && SUPABASE_ACCESS_TOKEN_REGEX.test(token)
}

export function isEmailOwner(value) {
  const email = normalizeString(value).toLowerCase()
  return Boolean(email) && EMAIL_REGEX.test(email)
}

export function normalizeSupabaseAccessTokenExp(value) {
  if (value === undefined || value === null || value === '') return null

  if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
    if (Object.prototype.hasOwnProperty.call(value, 'experimental')) {
      return normalizeSupabaseAccessTokenExp(value.experimental)
    }
    if (Object.prototype.hasOwnProperty.call(value, 'exp')) {
      return normalizeSupabaseAccessTokenExp(value.exp)
    }
    if (Object.prototype.hasOwnProperty.call(value, 'accessTokenExperimental')) {
      return normalizeSupabaseAccessTokenExp(value.accessTokenExperimental)
    }
    if (Object.prototype.hasOwnProperty.call(value, 'accessTokenExp')) {
      return normalizeSupabaseAccessTokenExp(value.accessTokenExp)
    }
  }

  const raw = normalizeString(value)
  if (!raw) return null
  if (!isSupabaseAccessToken(raw)) return null
  return raw
}

function hasRequiredLocalFields(extracted) {
  return Boolean(
    normalizeString(extracted?.endpoint)
    && normalizeString(extracted?.accessKeyId)
    && normalizeString(extracted?.secretAccessKey)
    && normalizeString(extracted?.bucketName),
  )
}

function extractRawSupabaseFields(rawInput) {
  const lowerCaseMap = parseKeyValueObjects(rawInput)
  const getFromMap = (key) => normalizeString(lowerCaseMap.get(key.toLowerCase()))

  const databaseRaw = getFromMap('supabase.com.database')
    || extractValueByKeys(rawInput, ['supabase.com.database', 'supabase.database'])

  const parsedDatabase = parseDatabaseDescriptor(databaseRaw)
  if (!parsedDatabase.endpoint || !parsedDatabase.accessKeyId || !parsedDatabase.secretAccessKey) {
    const directDescriptor = rawInput.match(/S3Url\s*:\s*https?:\/\/[^\s|"'`]+(?:\s*\|\s*AccessKeyID\s*:\s*[^\s|"'`]+)(?:\s*\|\s*SecretAccessKey\s*:\s*[^\s|"'`]+)/i)
    if (directDescriptor?.[0]) {
      const fallback = parseDatabaseDescriptor(directDescriptor[0])
      parsedDatabase.endpoint = parsedDatabase.endpoint || fallback.endpoint
      parsedDatabase.accessKeyId = parsedDatabase.accessKeyId || fallback.accessKeyId
      parsedDatabase.secretAccessKey = parsedDatabase.secretAccessKey || fallback.secretAccessKey
      parsedDatabase.bucketName = parsedDatabase.bucketName || fallback.bucketName
      parsedDatabase.region = parsedDatabase.region || fallback.region
    }
  }

  const accessToken = getFromMap('supabase.com.accesstoken')
    || extractValueByKeys(rawInput, ['supabase.com.accessToken', 'supabase.accessToken'])
    || extractFirstAccessToken(rawInput)

  const accessTokenExperimentalRaw = getFromMap('supabase.com.accesstoken.experimental')
    || getFromMap('supabase.com.accesstokenexp')
    || extractValueByKeys(rawInput, [
      'supabase.com.accessToken.experimental',
      'supabase.accessToken.experimental',
      'supabase.com.accessTokenExp',
      'supabase.accessTokenExp',
      'accessTokenExperimental',
      'accessTokenExp',
      'supabase_access_token_exp',
    ])

  const accessTokenExpRaw = getFromMap('supabase.com.accesstoken.exp')
    || getFromMap('supabase.accesstoken.exp')
    || extractValueByKeys(rawInput, [
      'supabase.com.accessToken.exp',
      'supabase.accessToken.exp',
      'supabase.accessToken.experimental',
      'supabase.accessTokenExp',
      'accessToken.experimental',
      'accessToken.exp',
      'supabase_access_token_exp',
    ])

  const emailOwnerRaw = getFromMap('emailowner')
    || getFromMap('supabase.com.emailowner')
    || extractValueByKeys(rawInput, ['emailOwner', 'supabase.com.emailOwner', 'ownerEmail', 'owner_email'])
    || extractFirstEmail(rawInput)

  const endpoint = normalizeEndpoint(parsedDatabase.endpoint)
  const endpointValidated = SUPABASE_S3_ENDPOINT_REGEX.test(endpoint) ? endpoint : ''
  const projectRef = deriveProjectRefFromEndpoint(endpointValidated)

  const accessKeyIdRaw = normalizeString(parsedDatabase.accessKeyId)
  const secretAccessKeyRaw = normalizeString(parsedDatabase.secretAccessKey)

  const accessKeyId = ACCESS_KEY_ID_REGEX.test(accessKeyIdRaw) ? accessKeyIdRaw : ''
  const secretAccessKey = SECRET_ACCESS_KEY_REGEX.test(secretAccessKeyRaw) ? secretAccessKeyRaw : ''

  const emailOwner = isEmailOwner(emailOwnerRaw) ? normalizeString(emailOwnerRaw).toLowerCase() : ''

  const bucketRaw = parsedDatabase.bucketName
    || extractValueByKeys(rawInput, ['bucketName', 'bucket', 'supabase.bucketName'])
  const bucketName = sanitizeBucketName(bucketRaw)
  const fallbackBucketName = deriveBucketFromEmail(emailOwner)
  const resolvedBucketName = bucketName || fallbackBucketName

  const experimentalToken = normalizeSupabaseAccessTokenExp(accessTokenExperimentalRaw)
  const expToken = normalizeSupabaseAccessTokenExp(accessTokenExpRaw) || experimentalToken

  const extracted = {
    endpoint: endpointValidated,
    projectRef,
    accessKeyId,
    secretAccessKey,
    accessToken: normalizeString(accessToken),
    accessTokenExperimental: experimentalToken || normalizeString(accessTokenExperimentalRaw),
    accessTokenExp: expToken,
    emailOwner,
    bucketName: resolvedBucketName,
    bucketNameSource: bucketName ? 'input' : (fallbackBucketName ? 'email_default' : 'none'),
    region: normalizeString(parsedDatabase.region) || '',
    accountId: deriveAccountId(emailOwner, projectRef),
    databaseRaw: normalizeString(databaseRaw),
  }

  const missingRequired = []
  if (!extracted.endpoint) missingRequired.push('supabase.com.database.S3Url')
  if (!extracted.accessKeyId) missingRequired.push('supabase.com.database.AccessKeyID')
  if (!extracted.secretAccessKey) missingRequired.push('supabase.com.database.SecretAccessKey')

  const warnings = []
  if (endpoint && !endpointValidated) {
    warnings.push('S3Url format is not a valid Supabase S3 endpoint')
  }
  if (accessKeyIdRaw && !accessKeyId) {
    warnings.push('AccessKeyID format is invalid')
  }
  if (secretAccessKeyRaw && !secretAccessKey) {
    warnings.push('SecretAccessKey format is invalid')
  }
  if (extracted.accessToken && !isSupabaseAccessToken(extracted.accessToken)) {
    warnings.push('supabase.com.accessToken format does not match expected sbp_* token')
  }
  if (accessTokenExperimentalRaw && !experimentalToken) {
    warnings.push('supabase.com.accessToken.experimental format is invalid')
  }
  if (!extracted.bucketName) {
    warnings.push('bucketName could not be derived from payload or emailOwner')
  }
  if (!extracted.emailOwner) {
    warnings.push('emailOwner was not found in input text')
  }

  return {
    service: 'supabaseS3',
    extracted,
    missingRequired,
    warnings,
    notes: [],
    canLookupRemote: isSupabaseAccessToken(extracted.accessToken),
  }
}

function parseJsonResponse(text) {
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}

function compactErrorDetail(payload, fallbackText = '') {
  if (payload && typeof payload === 'object') {
    return payload.message || payload.error || payload.msg || JSON.stringify(payload)
  }
  return normalizeString(fallbackText)
}

async function supabaseManagementRequest(path, accessToken, { method = 'GET', body } = {}) {
  const headers = {
    Authorization: `Bearer ${accessToken}`,
    Accept: 'application/json',
  }
  if (body !== undefined) {
    headers['content-type'] = 'application/json'
  }

  const response = await fetch(`${SUPABASE_MANAGEMENT_BASE_URL}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  })

  const text = await response.text()
  const payload = parseJsonResponse(text)

  if (!response.ok) {
    const detail = compactErrorDetail(payload, text)
    const message = `Supabase API ${method} ${path} failed (${response.status})${detail ? `: ${detail}` : ''}`
    const error = new Error(message)
    error.statusCode = response.status
    throw error
  }

  return payload
}

function findProject(projects, projectRef) {
  if (!Array.isArray(projects) || projects.length === 0) return null
  if (projectRef) {
    const found = projects.find((project) => normalizeString(project?.ref) === projectRef)
    if (found) return found
  }
  if (projects.length === 1) return projects[0]
  return null
}

function normalizeBucketItems(payload) {
  if (!Array.isArray(payload)) return []
  return payload
    .map((entry) => ({
      id: normalizeString(entry?.id),
      name: normalizeString(entry?.name),
      public: Boolean(entry?.public),
    }))
    .filter((entry) => entry.name)
}

export function createAccountDraftFromSupabase(preview, options = {}) {
  const extracted = preview?.extracted || {}
  const bucketName = sanitizeBucketName(options.bucketName || extracted.bucketName || '') || ''
  const requestedRegion = normalizeString(extracted.region || options.region || 'us-east-1')
  const signingRegion = resolveS3SigningRegion({
    endpoint: extracted.endpoint,
    region: requestedRegion,
  })

  return {
    accountId: normalizeString(extracted.accountId || ''),
    accessKeyId: normalizeString(extracted.accessKeyId || ''),
    secretAccessKey: normalizeString(extracted.secretAccessKey || ''),
    endpoint: normalizeString(extracted.endpoint || ''),
    region: signingRegion,
    bucket: bucketName,
    addressingStyle: 'path',
    payloadSigningMode: 'unsigned',
    emailOwner: normalizeString(extracted.emailOwner || '').toLowerCase(),
    supabaseAccessToken: normalizeString(extracted.accessToken || ''),
    supabaseAccessTokenExp: normalizeSupabaseAccessTokenExp(
      extracted.accessTokenExp || extracted.accessTokenExperimental,
    ),
  }
}

function isAccountDraftSufficient(accountDraft) {
  return Boolean(
    accountDraft
    && normalizeString(accountDraft.endpoint)
    && normalizeString(accountDraft.accessKeyId)
    && normalizeString(accountDraft.secretAccessKey)
    && normalizeString(accountDraft.bucket),
  )
}

export async function previewSupabaseS3(rawInput, options = {}) {
  const local = extractRawSupabaseFields(String(rawInput ?? ''))

  const response = {
    ...local,
    remote: {
      attempted: false,
      ok: false,
      fallbackToLocal: false,
      project: null,
      profile: null,
      buckets: [],
      bucketResolved: null,
      bucketCreated: false,
      error: '',
      profileWarning: '',
    },
    accountDraft: createAccountDraftFromSupabase(local),
  }

  const shouldLookup = options.lookupRemote === true
  if (!shouldLookup || !local.canLookupRemote) {
    return response
  }

  response.remote.attempted = true

  try {
    const accessToken = local.extracted.accessToken
    const projects = await supabaseManagementRequest('/projects', accessToken)
    const matchedProject = findProject(projects, local.extracted.projectRef)

    let profile = null
    let profileWarning = ''

    let bucketItems = []
    let bucketCreated = false
    const overrideBucket = sanitizeBucketName(options.bucketName || '')
    let bucketResolved = overrideBucket || sanitizeBucketName(local.extracted.bucketName || '') || ''

    if ((!local.extracted.emailOwner || !bucketResolved) && accessToken) {
      try {
        profile = await supabaseManagementRequest('/profile', accessToken)
      } catch (err) {
        profileWarning = err?.message ?? String(err)
      }
    }

    const profileEmail = normalizeString(profile?.primary_email || profile?.email).toLowerCase()
    const emailOwner = local.extracted.emailOwner || (isEmailOwner(profileEmail) ? profileEmail : '')

    if (!bucketResolved) {
      bucketResolved = deriveBucketFromEmail(emailOwner)
    }

    if (matchedProject?.ref) {
      const bucketsPath = `/projects/${encodeURIComponent(matchedProject.ref)}/storage/buckets`
      bucketItems = normalizeBucketItems(await supabaseManagementRequest(bucketsPath, accessToken))

      const bucketExists = bucketResolved
        ? bucketItems.some((bucket) => bucket.name === bucketResolved)
        : false

      if (!bucketExists && bucketResolved && options.createBucketIfMissing === true) {
        await supabaseManagementRequest(bucketsPath, accessToken, {
          method: 'POST',
          body: {
            name: bucketResolved,
            public: false,
          },
        })
        bucketCreated = true
        bucketItems = normalizeBucketItems(await supabaseManagementRequest(bucketsPath, accessToken))
      }
    }

    response.extracted = {
      ...response.extracted,
      emailOwner,
      bucketName: bucketResolved || response.extracted.bucketName,
      bucketNameSource: bucketResolved ? 'api_lookup' : response.extracted.bucketNameSource,
    }

    response.remote = {
      attempted: true,
      ok: true,
      fallbackToLocal: false,
      project: matchedProject ? {
        ref: normalizeString(matchedProject.ref),
        name: normalizeString(matchedProject.name),
        region: normalizeString(matchedProject.region),
        status: normalizeString(matchedProject.status),
      } : null,
      profile: profile ? {
        id: normalizeString(profile?.id),
        email: profileEmail || '',
      } : null,
      buckets: bucketItems.map((bucket) => bucket.name),
      bucketResolved: bucketResolved || null,
      bucketCreated,
      error: '',
      profileWarning,
    }

    if (profileWarning) {
      response.notes = [...response.notes, `Profile lookup skipped: ${profileWarning}`]
    }

    response.accountDraft = createAccountDraftFromSupabase(response, {
      bucketName: bucketResolved || undefined,
      region: normalizeString(matchedProject?.region || ''),
    })
    const projectRegion = normalizeString(matchedProject?.region || '')
    if (projectRegion && response.accountDraft.region !== projectRegion) {
      response.notes = [
        ...response.notes,
        `Project region is ${projectRegion}, but S3 signing region uses ${response.accountDraft.region} for compatibility.`,
      ]
    }
  } catch (err) {
    const errorMessage = err?.message ?? String(err)
    const fallbackToLocal = isAccountDraftSufficient(response.accountDraft)

    response.remote = {
      attempted: true,
      ok: false,
      fallbackToLocal,
      project: null,
      profile: null,
      buckets: [],
      bucketResolved: null,
      bucketCreated: false,
      error: errorMessage,
      profileWarning: '',
    }

    if (fallbackToLocal) {
      response.notes = [...response.notes, `Remote lookup failed, using local parsed data: ${errorMessage}`]
    } else {
      response.warnings = [...response.warnings, errorMessage]
    }
  }

  if (!hasRequiredLocalFields(response.extracted) && response.remote.error) {
    if (!response.warnings.includes(response.remote.error)) {
      response.warnings = [...response.warnings, response.remote.error]
    }
  }

  return response
}
