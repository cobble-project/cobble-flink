# Cobble Flink Docs

This directory hosts `cobble-flink` documentation using the `just-the-docs` Jekyll theme.

## Documentation structure

- `index.md`: introduction and project overview
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

`docs-pages.yml` publishes `latest/` when documentation changes reach `main` or
`master`. A release tag matching `v<cobble-version>-<patch>` publishes an
immutable versioned directory.

### How to operate

1. Merge documentation changes into `main` to update `latest/`.
2. Create and push a release tag such as `v0.3.0-1`.
3. The workflow publishes the tagged sources to `0.3.0-1/` on `gh-pages`.

### Versioned roots

- `<cobble-version>-<patch>/`
- `<cobble-version>/` redirects to its most recently published patch.
- `latest/` contains the documentation built from `main` or `master`.
- The site root redirects to `latest/`.

Example URLs:

- `https://cobble-project.github.io/cobble-flink/latest/`
- `https://cobble-project.github.io/cobble-flink/0.3.0-1/`
