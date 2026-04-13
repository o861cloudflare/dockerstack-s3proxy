# App service (`compose.apps.yml`) — S3 Proxy runtime

## Vai trò
- Service ứng dụng chính, build từ `services/s3proxy`.
- Cung cấp S3-compatible proxy + metadata/runtime metrics.

## Cấu hình chính
- Image local tag: `${PROJECT_NAME}-s3proxy:local`
- Build context: `./services/s3proxy`
- Port expose localhost: `127.0.0.1:${APP_HOST_PORT}:${APP_PORT}`
- Data volume: `${DOCKER_VOLUMES_ROOT:-./.docker-volumes}/s3proxy-data:/app/.docker-volumes/s3proxy-data`
- Healthcheck: `wget http://localhost:${APP_PORT}${HEALTH_PATH}`

## ENV bắt buộc
- `APP_PORT`: port proxy lắng nghe trong container.
- `S3PROXY_API_KEY`, `S3PROXY_FIREBASE_RTDB_URL`, `S3PROXY_FIREBASE_DB_SECRET`: bắt buộc cho nghiệp vụ s3proxy.
- `PROJECT_NAME`, `DOMAIN`: tạo hostname public.
- `CADDY_AUTH_USER`, `CADDY_AUTH_HASH`: basic auth.

## ENV optional
- `APP_HOST_PORT` (default 3000): truy cập localhost host machine.
- `NODE_ENV` (default production).
- `HEALTH_PATH` (default `/health`).
- `DOCKER_VOLUMES_ROOT` (default `./.docker-volumes`).
- `S3PROXY_SQLITE_PATH` (default `./.docker-volumes/s3proxy-data/routes.db`).
- `TAILSCALE_TAILNET_DOMAIN`: route HTTPS nội bộ qua caddy_1.

## Routing
- Public host: `${PROJECT_NAME}.${DOMAIN}` (+ alias).
- Internal HTTPS host: `${PROJECT_NAME}.${TAILSCALE_TAILNET_DOMAIN}` với `tls internal`.
