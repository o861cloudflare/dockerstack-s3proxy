# deploy.new.md

Huong dan thay the app/service moi voi rui ro thap nhat.

## Muc tieu
- Clone stack hien tai de deploy dich vu khac.
- Chi thay phan app ma khong pha core/ops/access.
- Chuan hoa du lieu runtime cua container vao `./.docker-volumes`.
- Filebrowser xem duoc toan bo data runtime song song voi `workspace`.

## Buoc 1 — Tao workspace moi tu template

```bash
node scripts/clone-stack.js --output /opt/stacks --name service-b
```

Ket qua: `/opt/stacks/service-b` chua ban sao repo (da bo `.git`).

## Buoc 2 — Thay app bang s3proxy

### Cach A: dung source `services/s3proxy` (khuyen nghi)
- Service `app` trong `compose.apps.yml` build tu `services/s3proxy`.
- Khong can giu `services/app`.
- Bao dam health check van dung `APP_PORT` + `HEALTH_PATH`.

### Cach B: dung image co san
- Sua service `app` trong `compose.apps.yml`:
  - doi `image` sang image thuc te.
  - bo/dieu chinh `build`.
  - map lai `APP_PORT` neu khac.

## Buoc 3 — Chuan hoa data vao `.docker-volumes`

Mac dinh stack dung:

- `${DOCKER_VOLUMES_ROOT:-./.docker-volumes}`

Quy uoc bat buoc khi them service moi:

1. Moi du lieu can persist cua container phai map ve host duoi:
   - `${DOCKER_VOLUMES_ROOT:-./.docker-volumes}/<service>/<du-lieu>:/path/in/container`
2. Khong dung named volume an danh cho du lieu can quan sat tren host.
3. Neu service co nhieu du lieu, tach thu muc ro rang (`config`, `data`, `db`, `logs`, `state`...).

Vi du:

```yaml
services:
  myapp:
    image: ghcr.io/org/myapp:latest
    volumes:
      - ${DOCKER_VOLUMES_ROOT:-./.docker-volumes}/myapp/data:/var/lib/myapp
      - ${DOCKER_VOLUMES_ROOT:-./.docker-volumes}/myapp/config:/etc/myapp
```

Thu muc goi y nen tao san:

```bash
mkdir -p .docker-volumes/s3proxy-data
mkdir -p .docker-volumes/caddy/data .docker-volumes/caddy/config
mkdir -p .docker-volumes/filebrowser/database
mkdir -p .docker-volumes/tailscale/var-lib
```

Ghi chu:
- `docker-compose/scripts/dc.sh` se tu dong `mkdir -p` cac thu muc data co ban tren truoc khi chay `docker compose`.
- Tao san thu muc van nen lam neu can set quyen truy cap chi tiet.

Neu nang cap tu template cu (dang dung named volume):
- Cac volume cu (`<project>_caddy_data`, `<project>_caddy_config`, `<project>_tailscale_data`, `<project>_filebrowser_data`) se khong duoc mount nua.
- Can migrate data sang `.docker-volumes/...` truoc khi cleanup named volume cu.

PowerShell:

```powershell
New-Item -ItemType Directory -Force `
  .docker-volumes/s3proxy-data, `
  .docker-volumes/caddy/data, `
  .docker-volumes/caddy/config, `
  .docker-volumes/filebrowser/database, `
  .docker-volumes/tailscale/var-lib | Out-Null
```

## Buoc 4 — Cap nhat env

Toi thieu:

- `PROJECT_NAME`
- `DOMAIN`
- `CADDY_EMAIL`
- `CADDY_AUTH_USER`
- `CADDY_AUTH_HASH`
- `APP_PORT`
- `S3PROXY_API_KEY`
- `S3PROXY_FIREBASE_RTDB_URL`
- `S3PROXY_FIREBASE_DB_SECRET`

Tuy chon:

- `APP_HOST_PORT`, `NODE_ENV`, `HEALTH_PATH`
- `S3PROXY_SQLITE_PATH`, `S3PROXY_QUOTA_*`, `S3PROXY_RECONCILE_INTERVAL_MS`, `S3PROXY_LOG_LEVEL`
- `ENABLE_*`
- `DOCKER_VOLUMES_ROOT` (mac dinh `./.docker-volumes`)
- Tailscale block neu can private access.

## Buoc 5 — Cloudflare

- Cap nhat `cloudflared/config.yml` theo hostname moi.
- Dam bao DNS record tro dung tunnel.

## Buoc 6 — Validate truoc khi chay

```bash
npm run dockerapp-validate:env
npm run dockerapp-validate:compose
```

Neu co loi `❌` -> bat buoc sua truoc khi deploy.

## Buoc 7 — Deploy

```bash
npm run dockerapp-exec:up
npm run dockerapp-exec:ps
npm run dockerapp-exec:logs
```

## Buoc 8 — Checklist hoan thanh theo tung lop

### Lop app
- `app` healthy.
- `http://127.0.0.1:${APP_HOST_PORT}` (neu co publish).
- `GET /metrics` co metric prefix `s3proxy_`.

### Lop public
- Host `${PROJECT_NAME}.${DOMAIN}` truy cap OK.
- Basic auth hoat dong.

### Lop ops (neu bat)
- `logs.*`, `files.*`, `ttyd.*` truy cap duoc.
- Trong filebrowser thay duoc:
  - `/srv/workspace`
  - `/srv/docker-volumes`

### Lop access (neu bat)
- Tailnet host noi bo truy cap duoc.
- Keep-ip logs khong bao loi Firebase/API.
- Truy cap ops bang hostname+port qua tailnet:
  - `http://${PROJECT_NAME}.${TAILSCALE_TAILNET_DOMAIN}:${DOZZLE_HOST_PORT:-18080}`
  - `http://${PROJECT_NAME}.${TAILSCALE_TAILNET_DOMAIN}:${FILEBROWSER_HOST_PORT:-18081}`
  - `http://${PROJECT_NAME}.${TAILSCALE_TAILNET_DOMAIN}:${WEBSSH_HOST_PORT:-17681}`

## Tong ket diem can doi khi thay dich vu

1. `compose.apps.yml` (image/build/port/health + env cua s3proxy).
2. Tat ca compose file co data volume: map vao `${DOCKER_VOLUMES_ROOT:-./.docker-volumes}/...`.
3. `.env` (identity + domain + auth + port + flags + PROXY/FIREBASE env).
4. `cloudflared/config.yml` (ingress hostnames).
5. Tuy chon: script CI/CD de reflect ten stack moi.
