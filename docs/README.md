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

`docs-pages.yml` publishes when a release tag matching
`v<cobble-version>-<patch>` is pushed. The Cobble version and Cobble Flink patch
form the versioned documentation directory.

### How to operate

1. Create a release tag such as `v0.2.3-1`.
2. Push the tag to GitHub.
3. The workflow builds the tagged sources and publishes them to
   `0.2.3-1/` on `gh-pages`.

### Versioned roots

- `<cobble-version>-<patch>/`
- `<cobble-version>/` redirects to its most recently published patch.
- `latest/` and the site root redirect to the most recently published tag.

Example URLs:

- `https://cobble-project.github.io/cobble-flink/latest/`
- `https://cobble-project.github.io/cobble-flink/0.2.3-1/`
