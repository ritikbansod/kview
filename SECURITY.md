# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |

---

## Security Model & Network Exposure

Kview is designed for **local development and trusted internal networks**.

### Authentication & Access Control
The Kview REST API and web UI currently do **not** have a built-in user authentication or authorization layer. Anyone with network access to the Kview server port (`8090` by default) can:
- Browse, tail, and search messages across all connected Kafka clusters.
- Produce messages to any topic.
- Create, modify, and delete topics.
- Reset consumer group offsets.

### Default Loopback Binding
To prevent inadvertent exposure on shared servers or cloud VMs, Kview binds to **`127.0.0.1`** by default (`server.address: ${KVIEW_BIND:127.0.0.1}`).
- When running in Docker containers, `KVIEW_BIND` is set to `0.0.0.0` so that ports can be forwarded explicitly.
- **Do not** expose port `8090` directly to the public Internet without protection.

### Recommended Production / Team Setup
If deploying Kview in a team environment or shared private cloud:
1. Deploy behind an authenticating reverse proxy such as:
   - [OAuth2-Proxy](https://oauth2-proxy.github.io/oauth2-proxy/)
   - [NGINX](https://nginx.org/) with `http_auth_basic_module` or OIDC
   - [Caddy](https://caddyserver.com/) with forward auth
   - [Cloudflare Access](https://www.cloudflare.com/products/zero-trust/access/) or AWS ALB / Azure App Gateway with authentication.
2. Restrict file permissions on the storage directory (`./data` by default), which contains `connections.json` and saved cluster credentials.

---

## Reporting a Vulnerability

If you discover a potential security vulnerability in Kview:

1. **Do not** open a public GitHub issue.
2. Please report the issue privately by emailing **ritikbansod.dev@gmail.com** or via [GitHub Security Advisory](https://github.com/ritikbansod/kview/security/advisories/new).
3. Include detailed steps to reproduce the issue, along with affected versions and any proof-of-concept scripts.
4. You will receive an acknowledgment within 48 hours, followed by updates regarding a fix and public disclosure timeline.
