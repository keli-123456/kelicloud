# 自用上线加固清单

这份清单适合先自用、后续再商业化的部署方式。目标是先把安全边界、备份和恢复做稳，功能迭代可以慢慢来。

## 基础环境

- 使用 Linux 服务器和 Docker Compose 部署。
- 使用 MySQL 或 MariaDB，不要把生产数据放在临时测试库里。
- 通过 Nginx、Caddy、Traefik 等反向代理提供 HTTPS。
- 只把反向代理暴露到公网，数据库端口不要暴露到公网。
- 管理员密码使用强密码，并保存到密码管理器。

## 必填环境变量

复制 `.env.example` 为 `.env` 后，至少改掉这些值：

```env
KOMARI_DB_HOST=your-mysql-host
KOMARI_DB_NAME=komari
KOMARI_DB_USER=komari
KOMARI_DB_PASS=replace-with-a-strong-password
ADMIN_USERNAME=admin
ADMIN_PASSWORD=replace-with-a-strong-password
KOMARI_CLOUD_SECRET_KEY=replace-with-a-stable-random-secret
```

`KOMARI_CLOUD_SECRET_KEY` 用于云主机 root 密码保险箱。这个值必须稳定保存；如果丢失，已经加密保存的 root 密码将无法解密。

## 登录保护

默认登录限流参数：

```env
KOMARI_LOGIN_MAX_FAILURES=8
KOMARI_LOGIN_WINDOW=10m
KOMARI_LOGIN_LOCKOUT=15m
```

含义是在 10 分钟窗口内，同一 IP 或同一 IP+用户名失败 8 次后锁定 15 分钟。自用部署可以保持默认值；公网商业化前建议接入反向代理层限速和日志告警。

## CORS 策略

系统设置里的 `allow_cors` 默认关闭。自用部署建议保持关闭。

如果确实需要跨域访问，先打开 `allow_cors`，再显式设置允许来源：

```env
KOMARI_CORS_ALLOWED_ORIGINS=https://monitor.example.com,https://admin.example.com
KOMARI_WS_ALLOWED_ORIGINS=
```

留空时只允许同主机 Origin。浏览器 WebSocket 会自动复用 `KOMARI_CORS_ALLOWED_ORIGINS`，只有需要单独放行额外 WebSocket 来源时才填写 `KOMARI_WS_ALLOWED_ORIGINS`。只有在你完全理解风险时才使用 `*`。

## HTTP 超时

默认值：

```env
KOMARI_HTTP_READ_HEADER_TIMEOUT=10s
KOMARI_HTTP_READ_TIMEOUT=0
KOMARI_HTTP_WRITE_TIMEOUT=0
KOMARI_HTTP_IDLE_TIMEOUT=120s
KOMARI_HTTP_MAX_BODY_BYTES=4194304
```

`ReadHeaderTimeout` 和 `IdleTimeout` 用于抵御慢连接占用。`WriteTimeout` 默认关闭，是为了避免影响 WebSocket、MJPEG 和较长时间的云厂商操作；如果你确认没有这些长连接/长请求，可以按需设置。`KOMARI_HTTP_MAX_BODY_BYTES` 默认限制普通 API 请求体为 4MB，备份上传和 favicon 上传保留各自的上传逻辑。

## 安全响应头

服务默认发送 `X-Content-Type-Options`、`X-Frame-Options`、`Referrer-Policy` 和 `Permissions-Policy`，降低浏览器侧误用风险。

如果你的站点已经稳定通过 HTTPS 访问，可以开启 HSTS：

```env
KOMARI_SECURITY_HSTS=true
```

只有在域名、证书和反向代理都确认无误后再开启。开启后浏览器会在一段时间内强制使用 HTTPS。

## 审计日志

管理员登录、终端连接、用户和客户端变更、云厂商凭据变更等动作会写入审计日志。查看明文客户端 token、云厂商 token/credential secret、云主机 root 密码、启用或关闭 2FA 也会以 `warn` 级别记录。

自用阶段建议定期查看审计日志；商业化前建议把审计日志纳入备份和异常告警。

## 备份和恢复

- 每天备份 MySQL/MariaDB。
- 同步备份 `.env`，尤其是 `KOMARI_CLOUD_SECRET_KEY`。
- 备份反向代理配置和证书配置。
- 每月至少做一次恢复演练，确认数据库备份和密钥能完整恢复。

## 上线前检查

```bash
docker compose up -d --build
docker compose logs -f komari
```

确认：

- 日志中没有数据库连接失败。
- 首次启动能创建管理员账号。
- HTTPS 域名可以打开前端。
- 登录失败多次后会返回 429。
- 超大的普通 API 请求会返回 413。
- 关闭 `allow_cors` 后跨域请求不会得到 CORS 响应头。
- 浏览器 WebSocket 只接受同主机或显式白名单来源。
- 响应头包含基础安全头；开启 HSTS 后 HTTPS 响应包含 `Strict-Transport-Security`。
- 查看敏感 token、credential secret 或 root 密码会进入审计日志。
- 云密码保险箱可用，并且 `KOMARI_CLOUD_SECRET_KEY` 已安全保存。
