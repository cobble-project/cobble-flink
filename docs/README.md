# Cobble Flink Docs

This directory hosts `cobble-flink` documentation using the `just-the-docs` Jekyll theme.

## Documentation structure

- `index.md`: home page
- `introduction/`: project overview and module map
- `getting-started/`: installation, Maven dependencies, and first run
- `state-backend/`: Cobble state backend and HA integration
- `source/`: Cobble SQL source connector
- `sink/`: Cobble SQL sink connector

## Local preview

```bash
cd docs
bundle install
bundle exec jekyll serve
```

Then open `http://127.0.0.1:4000`.

## GitHub Pages deployment

- Workflow files (repo root):
  - `.github/workflows/docs-ci.yml`
  - `.github/workflows/docs-pages.yml`
- `docs-pages.yml` is pure workflow deployment (no manual script).
- In repository settings, enable **Pages -> Deploy from a branch** and point to `gh-pages` / `(root)`.

### Trigger and root mapping

`docs-pages.yml` publishes in these cases:

1. Push to `main` / `master` with changes under `docs/**` -> publish `latest/`
2. Push a version tag with `v` prefix (for example `v0.2.0`) -> publish `<version>/`

All other refs are ignored by the deploy job.

### How to operate

1. For `latest/`: update docs and push to `main` / `master`.
2. For versioned docs: create and push a version tag (for example `v0.2.0`).
3. Workflow auto-builds and publishes to the corresponding root on `gh-pages`.

### Versioned roots

- `latest/`
- `<semver>/`

Example URLs:

- `https://cobble-project.github.io/cobble-flink/latest/`
- `https://cobble-project.github.io/cobble-flink/0.2.0/`
