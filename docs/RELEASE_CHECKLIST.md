# Release Checklist

## Pre-release

- Confirm CI is green on `main`.
- Run local smoke checks:
  - `python -m py_compile src/cluster_dispatch/cli.py`
  - `python -m unittest -v tests/test_cli_flows.py`
  - clean env install (`pip install .`) and `cdp --help`
- Review README examples against current CLI flags/commands.
- Review `pyproject.toml` version and update if needed.

## Release steps

1. Update version in `pyproject.toml`.
2. Commit: `chore: release vX.Y.Z`.
3. Tag release:
   - `git tag vX.Y.Z`
   - `git push origin vX.Y.Z`
4. Publish release notes.

## Release notes template

```markdown
## cluster-dispatch vX.Y.Z

### Highlights
- 

### New commands
- 

### Improvements
- 

### Fixes
- 

### Compatibility notes
- 
```
