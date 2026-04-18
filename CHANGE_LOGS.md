# CHANGE LOGS (Developer-facing)

---

## [2.0.0] — 2026-04-09

### Breaking Changes

- `docker-compose.yml` split into 4 module files — must use `docker-compose/scripts/dc.sh` (or `-f docker-compose/compose.core.yml -f docker-compose/compose.ops.yml -f docker-compose/compose.access.yml -f compose.apps.yml`) instead of plain `docker compose`
- Env var renames: `DOMAIN` replaces individual `SUBDOMAIN_*` vars; `STACK_NAME` replaces `COMPOSE_PROJECT_NAME`; `PROJECT_NAME` is new (required)
- `TAILSCALE_CLIENT_SECRET` → `TAILSCALE_AUTHKEY` (standardised Tailscale env naming)
- `APP_PORT` now drives the app container port directly; `SUBDOMAIN_APP`, `SUBDOMAIN_DOZZLE`, etc. removed

### Added

- **`docker-compose/scripts/dc.sh`** — main orchestrator: loads `.env`, reads `ENABLE_*` flags, builds `--profile` args, calls all 4 compose files in one command
- **`docker-compose/compose.core.yml`** — caddy + cloudflared, network + volumes definition; always-on
- **`docker-compose/compose.ops.yml`** — dozzle, filebrowser, webssh, webssh-windows; all profile-gated
- **`docker-compose/compose.access.yml`** — tailscale-linux, tailscale-windows; profile-gated
- **`compose.apps.yml`** — parameterised app service (`APP_IMAGE` + `APP_PORT`)
- **`docker-compose/scripts/up.sh` / `docker-compose/scripts/down.sh` / `docker-compose/scripts/logs.sh`** — one-liner shortcuts wrapping `dc.sh`
- **`docker-compose/scripts/validate-env.js`** — checks required vars, format validation (bcrypt, domain, port), subdomain preview
- **`docker-compose/scripts/validate-ts.js`** — Tailscale auth key format check + optional expiry lookup via TS API
- **`docker-compose/scripts/validate-compose.js`** — runs `docker compose config` across all 4 files to catch YAML errors
- **`npm run dockerapp-validate:all`** — combined validation pipeline (env → compose → TS)
- **`docs/DEPLOY.md`** — full deployment guide with mermaid flow diagrams, use cases, security checklist
- Subdomain auto-convention: all routes derived from `${PROJECT_NAME}.${DOMAIN}` pattern
- `DC_VERBOSE=1` debug flag for `docker-compose/scripts/dc.sh`
- `HEALTH_PATH` env to customise healthcheck endpoint per image

### Changed

- Image versions pinned (caddy `2.9.1-alpine`, cloudflared `2025.1.0`, dozzle `v8.x`, filebrowser `v2.30.0`, tailscale `stable`)
- Caddy `CADDY_INGRESS_NETWORKS` now uses `${STACK_NAME}_net` (was `app_net`)
- Network name: `${STACK_NAME:-mystack}_net` (dynamic, avoids conflicts between stacks)
- GitHub Actions and Azure Pipelines updated to call `docker-compose/scripts/dc.sh up` instead of bare `docker compose up`
- `detect-os.sh` no longer writes `COMPOSE_PROFILES` (profiles now fully managed by `docker-compose/scripts/dc.sh`)
- `.env.example` fully rewritten to match new schema

### Removed

- Monolithic `docker-compose.yml` (replaced by 4 module files)
- `SUBDOMAIN_APP`, `SUBDOMAIN_DOZZLE`, `SUBDOMAIN_FILEBROWSER`, `SUBDOMAIN_WEBSSH` env vars
- `TAILSCALE_CLIENT_SECRET` (use `TAILSCALE_AUTHKEY`)
- Hardcoded `build: ./services/app` in compose (now `APP_IMAGE` param)
- `scripts/generate-cf-config.js` and the generated-config workflow (maintain `cloudflared/config.yml` manually)

---

## [2.0.1] — 2026-04-16

### Added

- Admin UI chia theo tab: Tổng quan, Cron jobs, Accounts, Public Bucket Proxy, Tất cả file, Runtime, Logs
- Tab `Tất cả file` để xem toàn bộ file đã track theo account/scope, chọn file để replace hoặc delete trực tiếp
- API admin mới cho quản lý file track: list public files riêng, list all tracked files, replace file theo `encodedKey`, delete file theo `encodedKey`
- Báo cáo chạy cron chi tiết theo từng account khi bấm `run now`
- Toast thông báo thành công/thất bại và trạng thái loading cho các thao tác nặng trên form/admin actions
- Header sort cho bảng Accounts với mặc định sắp theo `addedAt` mới nhất

### Changed

- `admin/api/overview` không còn tải sẵn danh sách file public để tránh màn hình admin load chậm
- Tab `Public Bucket Proxy` chỉ tải danh sách khi bấm refresh, không fetch lúc load trang
- Lưu account thành công sẽ clear form account và phần `Thêm nhanh từ dịch vụ S3`
- Refresh dữ liệu theo từng tab mà không cần reload toàn trang

### Fixed

- Cải thiện UX cho thao tác cron, account, public file và managed file: có trạng thái đang xử lý, log rõ hơn và refresh cục bộ
- Đồng bộ metadata + usage về RTDB sau khi replace/delete tracked file từ admin

---

## 2026-04-16 ops image performance tune
- `services/webssh/Dockerfile`: đổi từ `tsl0922/ttyd:latest` + `apt-get install openssh-client` sang `tsl0922/ttyd:1.7.8-alpine` + `apk add --no-cache openssh-client-default` để giảm mạnh thời gian rebuild webssh.
- `docker-compose/compose.ops.yml`: pin `dozzle` sang `v10.3.3`, `webssh-windows` sang `alpine/socat:1.8.0.3`, thêm `pull_policy: missing` cho image ngoài, và đặt tên image local `dockerstack-webssh:local` cho service webssh để cache ổn định hơn.
- `docs/services/webssh.md`: bổ sung ghi chú vì sao chuyển sang Alpine và cách giảm thời gian build/pull.

## 2026-04-17 - built-in cron rows + external trigger API
- `services/s3proxy/src/cronScheduler.js`: thêm 3 built-in manual jobs (`probe_active_accounts`, `keepalive_touch`, `keepalive_scan`) luôn nạp sẵn vào runtime; không cần save cron mới để bấm `run now`.
- `services/s3proxy/src/cronScheduler.js`: khi `CRON_ENABLED=false` vẫn rebuild danh sách job để Admin UI và external API dùng được, chỉ tắt scheduler local interval.
- `services/s3proxy/src/cronScheduler.js`: thêm metadata `manualOnly`, `apiPath` và hỗ trợ override payload lúc trigger job.
- `services/s3proxy/src/routes/admin.js`: thêm protected endpoint `POST /api/cron-jobs/:jobId/run` dùng `x-api-key`/Bearer cho cron bên ngoài; endpoint admin cũ vẫn giữ để UI dùng trực tiếp.
- `services/s3proxy/src/admin-ui.html`: tab Cron jobs hiển thị rõ 3 built-in rows, phân biệt manual/API trigger-only, show endpoint trigger, khóa edit/delete với built-in jobs.
- `services/s3proxy/test/cron-api.test.js`: test built-in cron rows và external protected API.
## 2026-04-17
- Admin UI: thêm tab App access để mở nhanh domain / Tailscale / custom env URL cho S3Proxy, WebSSH, Dozzle, Filebrowser.
- Admin UI: thêm footer Runner info hiển thị 5 key chính và modal xem toàn bộ _DOTENVRTDB_RUNNER_*.
- Admin API /admin/api/overview: trả thêm runnerInfo và dockerAccess (inferred URLs + custom env URLs).
- compose.apps.yml / .env.example: bổ sung hướng dẫn pass _DOTENVRTDB_RUNNER_* và _DOCKER_ACCESS_URL_* vào container.

