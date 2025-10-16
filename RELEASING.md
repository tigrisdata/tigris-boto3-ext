# Release Process

This document describes the process for releasing new versions of `tigris-boto3-ext` to PyPI.

## Prerequisites

1. Ensure you have maintainer access to the PyPI project
2. All tests are passing on the `main` branch
3. The CHANGELOG has been updated with the new version's changes

## Release Steps

### 1. Update Version

Update the version number in `pyproject.toml`:

```toml
[project]
version = "x.y.z"
```

### 2. Create and Push a Git Tag

```bash
# Commit the version change
git add pyproject.toml
git commit -m "Bump version to x.y.z"

# Create a tag
git tag -a vx.y.z -m "Release vx.y.z"

# Push the commit and tag
git push origin main
git push origin vx.y.z
```

### 3. Automated Release

The GitHub Actions workflow (`.github/workflows/release.yml`) will automatically:

1. Build the distribution packages (source and wheel)
2. Validate the package metadata
3. Publish to PyPI using trusted publishing (OIDC)
4. Create a GitHub release with auto-generated release notes

### 4. Manual Release (if needed)

If you need to release manually:

```bash
# Install build dependencies
uv sync --all-extras

# Build the package
uv build

# Check the package
uv run twine check dist/*

# Upload to PyPI (requires PyPI token)
uv run twine upload dist/*
```

## PyPI Configuration

The project uses [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) for secure releases:

1. Go to PyPI project settings: https://pypi.org/manage/project/tigris-boto3-ext/settings/
2. Add a GitHub trusted publisher with:
   - Owner: `tigrisdata`
   - Repository: `tigris-boto3-ext`
   - Workflow: `release.yml`
   - Environment: `pypi`

## Versioning

This project follows [Semantic Versioning](https://semver.org/):

- **MAJOR** version for incompatible API changes
- **MINOR** version for backwards-compatible functionality
- **PATCH** version for backwards-compatible bug fixes

## Testing a Release

Before publishing, you can test the release process:

1. Build the package locally:
   ```bash
   uv build
   ```

2. Install in a clean environment:
   ```bash
   python -m venv test-env
   source test-env/bin/activate
   pip install dist/tigris_boto3_ext-*.whl
   ```

3. Test the installation:
   ```bash
   python -c "import tigris_boto3_ext; print(tigris_boto3_ext.__version__)"
   ```

## Post-Release

After a successful release:

1. Verify the package is available on PyPI: https://pypi.org/project/tigris-boto3-ext/
2. Test installation: `pip install tigris-boto3-ext`
3. Update documentation if needed
4. Announce the release (blog, social media, etc.)
