# cluster-dispatch

`cluster-dispatch` (`cdp`) is a Python CLI for dispatching and tracking analysis jobs across local and remote compute targets.

It supports:
- `none` (local execution)
- `sge`
- `univa`
- `pbs`
- `slurm`
- `lsf`

It is designed around an active analysis directory, explicit sync/pull behavior, and persistent local provenance records.

## Status

Pre-production, but already feature-rich and tested.

## Install

Requirements:
- Python `>=3.10`
- `rsync`
- `ssh` (only for SSH targets)

From source:

```bash
git clone https://github.com/koustav-pal/project-control.git
cd project-control
pip install -e .
```

Verify:

```bash
cdp --help
```

Shell completion:

```bash
cdp --install-completion
```

## Core model

- Project metadata lives in `.cluster_dispatch/`
- Sync excludes come from `.cdpignore` at project root
- One active target (`cdp target set`)
- One active analysis (`cdp analysis use`)
- Runs/sweeps write job records in `.cluster_dispatch/jobs/*.json`
- Sweeps also persist parent manifests in `.cluster_dispatch/sweeps/*.json`

Identity fields:
- `run_id`: deterministic run-definition identity
- `job_id`: scheduler/local execution identity
- `sweep_id`: parent sweep identity (sweep jobs only)

Schema contract:
- [`docs/JOB_RECORD_SCHEMA.md`](docs/JOB_RECORD_SCHEMA.md)

## Quickstart (local first)

Initialize project:

```bash
mkdir myproj && cd myproj
cdp init
```

Create and activate analysis:

```bash
mkdir -p analyses/demo
cdp analysis use analyses/demo
```

Run locally (`target=local`, `scheduler=none` by default):

```bash
cdp analysis run python -c "print('hello from cdp')"
```

Watch and inspect:

```bash
cdp watch --max-polls 20 --interval 2
cdp logs
cdp status
cdp history
```

## Remote target setup

Create a scheduler template file (user-supplied; placeholders required by your scheduler integration).

Required placeholders:
- `{cpus}`, `{memory}`, `{time}`, `{job_name}`, `{stdout}`, `{stderr}`, `{working_dir}`

Optional placeholders:
- `{queue}`, `{node}`, `{parallel_environment}`

Add target:

```bash
cdp target add cluster-a \
  --transport ssh \
  --host user@cluster.example \
  --scheduler slurm \
  --remote-root /scratch/user/myproj \
  --template-file slurm_header.tmpl
```

Switch target:

```bash
cdp target set cluster-a
```

## Sync model

Push active analysis to active target:

```bash
cdp sync push
cdp sync push --dry-run
```

Tag outputs for pull:

```bash
cdp analysis tag results
cdp analysis tag reports/figures
```

Pull tagged outputs:

```bash
cdp analysis pull
cdp analysis pull --remote
```

Or full pull:

```bash
cdp sync pull --all
```

Inspect sync history:

```bash
cdp sync status
cdp sync status --target cluster-a --action push --limit 10
cdp sync status --json
```

## Runs and sweeps

Single run:

```bash
cdp analysis run python train.py --epochs 20
cdp analysis run --dry-run python train.py --epochs 20
```

Retry a previous normal run:

```bash
cdp retry --job-id 12345
cdp retry --job-name run001 --dry-run
```

Sweep run:

```bash
cdp analysis sweep run --config sweep.yml \
  python train.py --lr {lr} --batch-size {batch_size}
```

Sweep controls:

```bash
cdp analysis sweep list
cdp analysis sweep show <sweep_id>
cdp analysis sweep resume <sweep_id>
cdp analysis sweep cancel <sweep_id>
```

## Validation and diagnostics

Project/target preflight:

```bash
cdp doctor
cdp target test local
cdp target test cluster-a --json
```

Run/sweep validation without submission:

```bash
cdp run validate python train.py --epochs 20
cdp run validate --json python train.py --epochs 20

cdp sweep validate --config sweep.yml \
  python train.py --lr {lr} --batch-size {batch_size}
```

## Monitoring and control

```bash
cdp status
cdp status --target cluster-a
cdp status list
cdp status global

cdp logs --job-id 12345
cdp watch --job-id 12345 --interval 10

cdp cancel --job-id 12345
cdp collect --job-id 12345
cdp stats --job-id 12345
```

## Provenance, reporting, cleanup

Config/context snapshot:

```bash
cdp config show
cdp config export --format json
cdp config export --format yaml
```

Operational summary:

```bash
cdp report
cdp report --target cluster-a --days 7 --json
```

Metadata cleanup:

```bash
cdp cleanup records --dry-run --older-than-days 30 --keep-last 200
cdp cleanup records --apply --jobs --sync
```

## Export / import metadata

Export project metadata bundle:

```bash
cdp export --output backup.tar.gz --jobs --sync --sweeps
cdp export --redact-hosts --output redacted.tar.gz
```

Import bundle:

```bash
cdp import backup.tar.gz
cdp import backup.tar.gz --overwrite
```

## Command map

Top-level command groups and commands:
- `analysis`: `use`, `tag`, `list`, `run`, `pull`, `sweep ...`
- `target`: `add`, `set`, `list`, `remove`, `test`
- `profile`: `list`, `show`, `set`, `delete`
- `ignore`: `list`, `add`, `remove`
- `status`: (default), `list`, `global`
- `sync`: `push`, `pull`, `status`
- `config`: `show`, `export`
- `cleanup`: `records`
- `run`: `validate`
- `sweep`: `validate`
- standalone: `init`, `doctor`, `logs`, `watch`, `history`, `report`, `retry`, `cancel`, `collect`, `stats`, `export`, `import`

## Migration notes

If you used earlier internal naming:
- command `pc` -> `cdp`
- `.project_control/` -> `.cluster_dispatch/`
- `.pcignore` -> `.cdpignore`

## Troubleshooting

`No active analysis set`:

```bash
cdp analysis use <path>
```

Target not found:

```bash
cdp target list
cdp target set <name>
```

Template placeholder errors:
- ensure required placeholders exist
- set `--queue` / `--parallel-environment` if template references them

Local install smoke test in restricted network environments:
- CI/packaging may require build backend dependencies (`setuptools`, `wheel`) available in your environment.

## Security notes

- Commands and scripts are executed locally or remotely via SSH.
- Review templates and command strings before submission.
- Use least-privilege credentials and hardened SSH config.

## Contributing

- Open an issue/PR with expected CLI behavior and compatibility impact.
- Release process checklist:
  - [`docs/RELEASE_CHECKLIST.md`](docs/RELEASE_CHECKLIST.md)
