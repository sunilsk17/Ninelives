# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's
[private vulnerability reporting](https://github.com/sunilsk17/Ninelives/security/advisories/new)
for this repository, not in public issues. Include what you found, how to reproduce it, and the
version affected. Reports are acknowledged as soon as possible and fixed with priority.

## Supported versions

Security fixes go into the latest release.

## Deployment notes

- **The HTTP API and dashboard have no authentication unless you set a token.**
  `ninelives serve` binds to `127.0.0.1` by default. Before exposing it, set `NINELIVES_API_TOKEN`
  (or `--token`), and serve it over HTTPS behind a reverse proxy. `/api/health` stays open, for
  probes.
- **Anyone who can reach the database controls the workflows.** They can read inputs and outputs,
  start runs and send signals. Restrict database credentials the way you would for your
  application's own data.
- **Inputs, outputs, step results and events are stored as JSON in plain text.** Do not pass secrets
  through them; keep secrets in your secret manager and pass references.
- **The dashboard keeps the API token in the browser's local storage** for convenience. Use a
  dedicated token and rotate it if a browser is shared.
- **Workflow code is imported and executed by workers.** The `APP` argument to the CLI is trusted
  input.
