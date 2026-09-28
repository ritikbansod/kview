# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |

---

## Security Model & Network Exposure

Kview is designed for **local development and trusted internal networks**.

### Authentication & Access Control
The REST API ships built-in bearer-token authentication (`kview.auth.*`), **disabled by default**:

| `KVIEW_AUTH_MODE` | Behavior |
|---|---|
| `none` (default) | API is open — acceptable on the default loopback binding for a single-user machine. |
| `token` | Static tokens from `KVIEW_AUTH_TOKENS` (`TOKEN` = admin, `TOKEN:readonly` = read-only). |
| `oidc` | JWTs validated against any OIDC issuer (`spring.security.oauth2.resourceserver.jwt.issuer-uri`); a configurable claim separates admin from read-only. |

In authenticated modes: every request needs `Authorization: Bearer <token>`; **read-only** covers
dashboard, browsing, tailing and schema decoding, while produce, topic create/delete/edit, offset
resets, group deletion and connection management require the **admin** role. CORS is closed unless
`KVIEW_AUTH_ALLOWED_ORIGINS` lists browser origins. The web UI prompts for the token (🔑 in the top
bar), the CLI takes `--token` / `KVIEW_TOKEN`, and the MCP server takes `KVIEW_TOKEN`. Cluster
secrets stay server-side and masked in responses regardless of mode.

### Default Loopback Binding
To prevent inadvertent exposure on shared servers or cloud VMs, Kview binds to **`127.0.0.1`** by default (`server.address: ${KVIEW_BIND:127.0.0.1}`).
- When running in Docker containers, `KVIEW_BIND` is set to `0.0.0.0` so that ports can be forwarded explicitly.
- **Do not** expose port `8090` directly to the public Internet without enabling the built-in authentication or placing Kview behind a proxy.

### Recommended Production / Team Setup
If deploying Kview in a team environment or shared private cloud:
1. Enable built-in authentication (`KVIEW_AUTH_MODE=token` or `oidc`) — for OIDC you can still front Kview with your identity provider of choice.
2. Alternatively/ additionally, deploy behind an authenticating reverse proxy such as:
   - [OAuth2-Proxy](https://oauth2-proxy.github.io/oauth2-proxy/)
   - [NGINX](https://nginx.org/) with `http_auth_basic_module` or OIDC
   - [Caddy](https://caddyserver.com/) with forward auth
   - [Cloudflare Access](https://www.cloudflare.com/products/zero-trust/access/) or AWS ALB / Azure App Gateway with authentication.
3. Restrict file permissions on the storage directory (`./data` by default), which contains `connections.json` and saved cluster credentials.

---

## Reporting a Vulnerability

If you discover a potential security vulnerability in Kview:

1. **Do not** open a public GitHub issue.
2. Please report the issue privately by emailing **ritikbansod.dev@gmail.com** or via [GitHub Security Advisory](https://github.com/ritikbansod/kview/security/advisories/new).
3. Include detailed steps to reproduce the issue, along with affected versions and any proof-of-concept scripts.
4. You will receive an acknowledgment within 48 hours, followed by updates regarding a fix and public disclosure timeline.
