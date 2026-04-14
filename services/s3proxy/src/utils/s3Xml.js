/**
 * src/utils/s3Xml.js
 * Build S3-compatible XML responses.
 */

const XML_DECLARATION = '<?xml version="1.0" encoding="UTF-8"?>'

function esc(str) {
  return String(str ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;')
}

function encodeIfNeeded(value, encodingType = '') {
  if (encodingType !== 'url') return value
  return encodeURIComponent(String(value ?? ''))
}

export function buildErrorXml(code, message, requestId = '') {
  return [
    XML_DECLARATION,
    '<Error>',
    `  <Code>${esc(code)}</Code>`,
    `  <Message>${esc(message)}</Message>`,
    `  <RequestId>${esc(requestId)}</RequestId>`,
    `  <HostId>${esc(requestId)}</HostId>`,
    '</Error>',
  ].join('\n')
}

export function buildListBucketResult(bucket, objects = [], options = {}) {
  const {
    prefix = '',
    delimiter = '',
    maxKeys = 1000,
    continuationToken = '',
    nextContinuationToken = '',
    startAfter = '',
    isTruncated = false,
    commonPrefixes = [],
    keyCount = objects.length + commonPrefixes.length,
    encodingType = '',
  } = options

  const contents = objects.map((obj) => {
    const lastMod = obj.lastModified
      ? new Date(obj.lastModified).toISOString()
      : new Date().toISOString()
    const etag = obj.etag ? `&quot;${obj.etag.replace(/"/g, '')}&quot;` : ''

    return [
      '  <Contents>',
      `    <Key>${esc(encodeIfNeeded(obj.key, encodingType))}</Key>`,
      `    <LastModified>${lastMod}</LastModified>`,
      etag ? `    <ETag>${etag}</ETag>` : '    <ETag></ETag>',
      `    <Size>${obj.size ?? 0}</Size>`,
      `    <StorageClass>${esc(obj.storageClass ?? 'STANDARD')}</StorageClass>`,
      '  </Contents>',
    ].join('\n')
  }).join('\n')

  const prefixNodes = commonPrefixes.map((entry) => (
    [
      '  <CommonPrefixes>',
      `    <Prefix>${esc(encodeIfNeeded(entry, encodingType))}</Prefix>`,
      '  </CommonPrefixes>',
    ].join('\n')
  )).join('\n')

  const lines = [
    XML_DECLARATION,
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
    `  <Name>${esc(bucket)}</Name>`,
    `  <Prefix>${esc(encodeIfNeeded(prefix, encodingType))}</Prefix>`,
    `  <MaxKeys>${maxKeys}</MaxKeys>`,
    `  <KeyCount>${keyCount}</KeyCount>`,
    `  <IsTruncated>${isTruncated}</IsTruncated>`,
  ]

  if (delimiter) lines.push(`  <Delimiter>${esc(encodeIfNeeded(delimiter, encodingType))}</Delimiter>`)
  if (startAfter) lines.push(`  <StartAfter>${esc(encodeIfNeeded(startAfter, encodingType))}</StartAfter>`)
  if (encodingType) lines.push(`  <EncodingType>${esc(encodingType)}</EncodingType>`)
  if (continuationToken) lines.push(`  <ContinuationToken>${esc(continuationToken)}</ContinuationToken>`)
  if (nextContinuationToken) lines.push(`  <NextContinuationToken>${esc(nextContinuationToken)}</NextContinuationToken>`)
  if (contents) lines.push(contents)
  if (prefixNodes) lines.push(prefixNodes)

  lines.push('</ListBucketResult>')
  return lines.join('\n')
}

export function buildInitiateMultipartUploadResult(bucket, key, uploadId) {
  return [
    XML_DECLARATION,
    '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
    `  <Bucket>${esc(bucket)}</Bucket>`,
    `  <Key>${esc(key)}</Key>`,
    `  <UploadId>${esc(uploadId)}</UploadId>`,
    '</InitiateMultipartUploadResult>',
  ].join('\n')
}

export function buildCompleteMultipartUploadResult(bucket, key, location, etag) {
  return [
    XML_DECLARATION,
    '<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
    `  <Location>${esc(location)}</Location>`,
    `  <Bucket>${esc(bucket)}</Bucket>`,
    `  <Key>${esc(key)}</Key>`,
    `  <ETag>&quot;${esc(etag?.replace(/"/g, '') ?? '')}&quot;</ETag>`,
    '</CompleteMultipartUploadResult>',
  ].join('\n')
}

export function buildGetBucketLocationResult(locationConstraint = '') {
  return [
    XML_DECLARATION,
    '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
    `  ${esc(locationConstraint)}`,
    '</LocationConstraint>',
  ].join('\n')
}

export function buildGetBucketVersioningResult(status = '') {
  const lines = [
    XML_DECLARATION,
    '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
  ]
  if (status) lines.push(`  <Status>${esc(status)}</Status>`)
  lines.push('</VersioningConfiguration>')
  return lines.join('\n')
}

export function buildListAllMyBucketsResult(buckets = [], owner = { id: 's3proxy', displayName: 's3proxy' }) {
  const bucketNodes = buckets.map((bucket) => {
    const createdAt = bucket.createdAt
      ? new Date(bucket.createdAt).toISOString()
      : new Date().toISOString()

    return [
      '    <Bucket>',
      `      <Name>${esc(bucket.name)}</Name>`,
      `      <CreationDate>${esc(createdAt)}</CreationDate>`,
      '    </Bucket>',
    ].join('\n')
  }).join('\n')

  return [
    XML_DECLARATION,
    '<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
    '  <Owner>',
    `    <ID>${esc(owner.id ?? 's3proxy')}</ID>`,
    `    <DisplayName>${esc(owner.displayName ?? 's3proxy')}</DisplayName>`,
    '  </Owner>',
    '  <Buckets>',
    bucketNodes,
    '  </Buckets>',
    '</ListAllMyBucketsResult>',
  ].join('\n')
}

export function buildDeleteObjectsResult(deleted = [], errors = []) {
  const deletedNodes = deleted.map((key) => (
    [
      '  <Deleted>',
      `    <Key>${esc(key)}</Key>`,
      '  </Deleted>',
    ].join('\n')
  )).join('\n')

  const errorNodes = errors.map((entry) => (
    [
      '  <Error>',
      `    <Key>${esc(entry.key)}</Key>`,
      `    <Code>${esc(entry.code)}</Code>`,
      `    <Message>${esc(entry.message)}</Message>`,
      '  </Error>',
    ].join('\n')
  )).join('\n')

  return [
    XML_DECLARATION,
    '<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
    deletedNodes,
    errorNodes,
    '</DeleteResult>',
  ].filter(Boolean).join('\n')
}
