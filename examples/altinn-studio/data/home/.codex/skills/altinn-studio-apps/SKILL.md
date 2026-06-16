---
name: altinn-studio-apps
description: Work with Altinn Studio apps locally using studioctl and Localtest. Use for installing or building studioctl, cloning Altinn Studio app repos, starting the local Altinn 3 test platform, running an app with `studioctl run` or `studioctl app run`, opening `http://local.altinn.cloud:8000/`, selecting app/login/test user, proceeding into an app, filling out Studio form workflows, diagnosing local runtime issues, or updating/verifying local development docs for user-facing Altinn Studio behavior.
---

# Altinn Studio apps

## Overview

Use `studioctl` as the preferred entrypoint for local Altinn Studio app development and testing. Keep app repositories under `/data/home/code/apps` unless the user gives another location.

Primary source references:

- CLI README: `/data/home/code/altinn-studio/src/cli/README.md`
- CLI Makefile: `/data/home/code/altinn-studio/src/cli/Makefile`
- End-user docs source: `/data/home/code/altinn-studio-docs/content/altinn-studio/v8/guides/development/local-dev/_index.en.md`
  - substitute `v8` depending on app version (there is currently only v10, and it is not correct yet, should be v9, so use v8 for now)
- v9 of app backend and frontend is in `/data/home/code/altinn-studio` (version was unified)
  - For v9, app frontend is managed and served from backend library: `src/App/backend/`
  - Source for frontend is `src/App/frontend`
  - A given app version is decided from `PackageReference` in `App.csproj`
- v4 and v8 of app frontend and app backend respectively is in `/data/home/code/app-frontend-react` and `/data/home/code/app-lib-dotnet`
  - A given app backend version is decided from `PackageReference` in `App.csproj`, and frontend version from `Index.cshtml`

If exact flags or behavior matter, run `studioctl -h` and the relevant subcommand help before deciding. The CLI is actively developed.

## Install studioctl

Prefer the published installer when validating user workflows:

```sh
curl -sSL https://altinn.studio/designer/api/v1/studioctl/install.sh | sh
```

Pin a specific version when the task depends on one:

```sh
curl -sSL https://altinn.studio/designer/api/v1/studioctl/install.sh | sh -s -- --version v0.1.0-preview.12
```

When developing `studioctl` itself from the monorepo, install from source:

```sh
cd /data/home/code/altinn-studio/src/cli
make user-install
```

Useful contributor checks in that folder are `make build`, `make fmt`, `make lint`, and `make test`.

Important notes:
- Always use `studioctl` from the global PATH, as that is what the end users do.
- Only invoke from dev-install when there is very good reason.
- `studioctl` is meant to support
  - Windows, macOS and Linux
  - Docker, podman, colima (and their "Desktop" counterparts)

## Clone or locate apps

Cloning apps requires login, but your harness comes pre-logged in.
Use `/data/home/code/apps` for app repositories:

```sh
ls /data/home/code/apps
```

Clone through `studioctl` when possible:

```sh
studioctl app clone [--env <env>] <org>/<repo>
```

Most apps are in production environment, so omit `--env` by default.

## Run localtest and app

Start the local Altinn 3 platform:

```sh
studioctl env up
```

Run the app from the app repository (`/data/home/code/apps/<app-repo>`):

```sh
studioctl run
```

Or point to an app folder explicitly:

```sh
studioctl run -p /data/home/code/apps/<app-repo>
```

`studioctl app run` is the longer form of `studioctl run`. It wraps running the app backend and auto-detects the app directory when possible.

Useful diagnostics:

```sh
studioctl doctor
studioctl app logs
studioctl env status
studioctl env logs
studioctl server status
studioctl server logs
```

Stop Localtest when finished or when resetting the local platform:

```sh
studioctl env down
```

Delete localtest data if you want a clean slate:

```sh
studioctl env reset
```

### Old localtest

The old version of localtest is in the `/data/home/code/app-localtest` repo.
It is based on podman/docker compose files.
If working on app-related bug report related to local development, we may need to clarify which localtest version was used.

## Browser testing

Use Playwright MCP for browser verification and using the app through its frontend.

1. Open `http://local.altinn.cloud:8000/` (localtest must be running, through `studioctl env up`).
2. Select the app. If only one app is running, it is usually selected by default.
3. Select login/test user.
4. Click `Proceed to app`.
5. Fill out the Studio app form according to the scenario.
6. Verify the user-visible behavior, capture screenshots and download datalement PDF/receipt if useful.

Studio apps are usually forms. Treat validation messages, prefill behavior, navigation, submission flow, accessibility, and browser console errors as part of the manual verification surface when relevant.

For C# changes, restart the app after editing. 
For JSON/layout/config changes, a browser reload is often enough. For prefill changes, create a new instance by returning to Localtest and logging in again.

## Documentation rule

When a product change affects user-facing Altinn Studio behavior, check whether docs must be updated in `altinn-studio-docs`. 
If local testing reveals errors, drift, or misalignment in the docs, correct those docs as part of the same work or explicitly report the follow-up if it cannot be done immediately.
