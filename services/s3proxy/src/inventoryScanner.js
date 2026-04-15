/**
 * src/inventoryScanner.js
 * Shared backend inventory scanner for quota polling and reconciliation.
 */

import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3'
import config from './config.js'
import { resolveS3SigningRegion } from './s3Signing.js'

export function createS3Client(account) {
  const signingRegion = resolveS3SigningRegion({
    endpoint: account.endpoint,
    region: account.region,
  })

  return new S3Client({
    endpoint: account.endpoint,
    region: signingRegion,
    credentials: {
      accessKeyId: account.access_key_id,
      secretAccessKey: account.secret_key,
    },
    forcePathStyle: true,
  })
}

export async function scanAccountInventory(account, options = {}) {
  const {
    continuationToken = undefined,
    client = createS3Client(account),
    onObject = async () => {},
    onPage = async () => {},
    maxPages = Number.POSITIVE_INFINITY,
    pageSize = config.INVENTORY_SCAN_PAGE_SIZE,
  } = options

  let token = continuationToken
  let totalBytes = 0
  let objectCount = 0
  let pageCount = 0
  let completed = false

  while (pageCount < maxPages) {
    const response = await client.send(new ListObjectsV2Command({
      Bucket: account.bucket,
      MaxKeys: pageSize,
      ContinuationToken: token,
    }))

    pageCount += 1
    const objects = []

    for (const object of response.Contents ?? []) {
      const record = {
        backendKey: object.Key ?? '',
        sizeBytes: object.Size ?? 0,
        etag: object.ETag?.replace(/"/g, '') ?? null,
        lastModified: object.LastModified ? new Date(object.LastModified).getTime() : null,
      }

      objects.push(record)
      totalBytes += record.sizeBytes
      objectCount += 1
      await onObject(record)
    }

    token = response.IsTruncated ? response.NextContinuationToken : undefined
    completed = !token

    await onPage({
      account,
      objects,
      totalBytes,
      objectCount,
      pageCount,
      nextContinuationToken: token ?? null,
      completed,
    })

    if (completed) break
  }

  return {
    totalBytes,
    objectCount,
    pageCount,
    nextContinuationToken: token ?? null,
    completed,
  }
}
