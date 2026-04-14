# s3proxy-http test flow

Run in order:

1. `00-prepare-smoke.http`
2. `01-admin-accounts.http`
3. `02-core-s3-crud.http`
4. `03-multipart.http`
5. `04-cleanup.http`

Notes:

- All files use domain URL only (`https://...`), no localhost.
- Domain/API key are read from `.env` via `{{$dotenv ...}}`.
- Keep `@run_id` the same across all files for one test cycle.
- In `02-core-s3-crud.http`, copy `NextContinuationToken` from request 14 into `@pagination_continuation_token` before request 15.
- In `03-multipart.http`, copy `UploadId`/`ETag` into placeholder variables before complete/abort requests.
- Replace account credentials/endpoints in `01-admin-accounts.http` with real backend values before running.

