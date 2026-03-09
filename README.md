# cluster-dispatch

`cluster-dispatch` is a Python CLI for running analysis jobs on remote SSH compute targets (SGE, Univa, PBS Pro, Slurm, LSF, or no scheduler), while keeping your local analysis directory as the source of truth.

It is designed for workflows where you:
- keep multiple analyses under one project,
- run jobs across multiple clusters/servers,
- track job status from your laptop,
- pull only tagged outputs back to local.

## Why this exists

HPC workflows often become hard to manage when commands, job scripts, paths, and output pulls are done ad hoc across many servers.

`cluster-dispatch` gives you a repeatable interface:
- one project-level config,
- explicit target definitions,
- analysis-level tagging for pull,
- scheduler template rendering with runtime/default resources.

## Core concepts

- `Project root`: directory where `cdp init` is run.
- `Target`: named remote execution profile (host + scheduler + remote root + default resources + template).
- `Active analysis`: local directory selected by `cdp analysis use`.
- `Tagged paths`: paths inside active analysis selected by `cdp analysis tag`; `cdp analysis pull` only pulls these.
- `Remote analysis root`: `<remote_root>/<active_analysis_path>`

No timestamped run directories are used. Remote structure mirrors local analysis path.

Local mode note: when target is `scheduler=none` with `host=localhost`/`127.0.0.1`, execution is local (no SSH).

## Current status behavior

`cdp status` is context-aware:
- inside active analysis directory: shows latest job status,
- outside active analysis directory: shows global status across targets.

You can always force target-specific status:
- `cdp status --target <target_name>`

## Requirements

- Python 3.10+
- `ssh` installed and configured
- `rsync` installed
- reachable remote hosts (directly or via SSH config aliases)

Recommended:
- configure host aliases in `~/.ssh/config`

## Installation

### From source (recommended currently)

```bash
git clone https://github.com/koustav-pal/project-control.git
cd project-control
pip install -e .
```

### Verify

```bash
cdp --help
```

## Quick start

### 1. Create a scheduler template

Example SGE template (`my_sge_template.tmpl`):

```bash
#$ -N {job_name}
#$ -o {stdout}.out
#$ -e {stderr}.err
#$ -l h_vmem={memory}
#$ -l h_rt={time}
#$ -pe {parallel_environment} {cpus}
#$ -q {queue}
```

Required placeholders:
- `{cpus}`, `{memory}`, `{time}`, `{job_name}`, `{stdout}`, `{stderr}`, `{working_dir}`

Optional placeholders:
- `{queue}`, `{node}`, `{parallel_environment}`

`{stdout}` and `{stderr}` default to the resolved `job_name`.
`{working_dir}` resolves to the target `remote_root`.

### 2. Initialize the project

```bash
cdp init
cdp init myproj
cdp init myproj --with-git
```

What this does:
- creates `.cluster_dispatch/config.yml`
- creates local state dirs under `.cluster_dispatch/`
- creates `.cdpignore` at project root (if missing)
- creates default target `local` (`scheduler=none`, `host=localhost`, `remote_root=<project_root>`)
- with `--with-git`:
  - initializes Git if no repository is detected
  - does not reinitialize if already inside a Git repository
  - creates `.gitignore` if missing (does not overwrite existing file)

Then add remote targets explicitly with `cdp target add`.

### 3. Set analysis and tags

```bash
cdp analysis use analyses/run_001
cdp analysis tag results
cdp analysis tag reports/figures
cdp analysis tag --remote remote_only_outputs
cdp analysis list
cdp analysis list --all
cdp analysis list --remote
cdp analysis list --remote --all
```

### 4. Run job

Using target defaults only:

```bash
cdp analysis run python train.py --epochs 20
```

Override resources at runtime:

```bash
cdp analysis run \
  --profile small \
  --cpus 8 \
  --memory 32G \
  --time 04:00:00 \
  --queue normal \
  --parallel-environment smp \
  --job-name run001 \
  python train.py --epochs 20
```

### 4b. Sweep jobs

```bash
cdp analysis sweep run --config sweep.yml \
  python train.py --lr {lr} --batch-size {batch_size}
```

Run one block only:

```bash
cdp analysis sweep run --config sweep.yml --job job1 \
  python train.py --lr {lr} --batch-size {batch_size}
```

### 5. Monitor and pull

```bash
cdp status
cdp status --job-id 123456
cdp status --job-name run001
cdp status --target cluster-a
cdp history
cdp history --analysis analyses/run_001 --limit 20
cdp logs
cdp logs --job-id 123456 --tail 100
cdp logs --job-name run001 --head 50
cdp logs --follow
cdp cancel --job-id 123456
cdp cancel --job-name run001 --target cluster-a
cdp stats --job-id 123456
cdp stats --job-name run001
cdp collect --job-id 123456
cdp collect --job-name run001
cdp doctor
cdp doctor --target cluster-a
cdp doctor --no-remote
cdp analysis sweep run --config sweep.yml --mode single python train.py --lr {lr} --batch-size {batch_size}
cdp analysis sweep run --config sweep.yml --mode local python train.py --lr {lr} --batch-size {batch_size}
cdp profile list
cdp analysis sweep list
cdp analysis sweep show sweep-20260101010101-abc123
cdp analysis sweep resume sweep-20260101010101-abc123
cdp analysis sweep cancel sweep-20260101010101-abc123
cdp status list --analysis analyses/run_001
cdp status global
cdp analysis pull
```

## Multi-target workflow

Add another target:

```bash
cdp target add cluster-b \
  --transport ssh \
  --host cluster-b \
  --scheduler pbs \
  --remote-root /scratch/me/projects
```

Switch active target:

```bash
cdp target set cluster-b
```

List targets:

```bash
cdp target list
```

## Resource resolution order

For each run resource (`cpus`, `memory`, `time`, `node`, `queue`, `parallel_environment`):
1. runtime flag (if provided),
2. selected profile values (if `--profile` is set),
3. target default from config.

If template contains `{queue}` or `{parallel_environment}`, those values must resolve non-empty (runtime or default), otherwise run fails fast.

## Command reference

### `cdp init`
Initializes cluster-dispatch in current directory or an optional project path.

Creates `.cluster_dispatch/config.yml`, state directories, and a default `local` target.

Examples:

```bash
cdp init
cdp init myproj
cdp init myproj --with-git
```

### `cdp target add <name>`
Adds/updates a target by name.

Key options:
- `--transport` (`ssh|local`)
- `--host`
- `--scheduler`
- `--remote-root`
- `--template-file`
- resource defaults:
  - `--cpus`
  - `--memory`
  - `--time`
  - `--node`
  - `--queue`
  - `--parallel-environment`

### `cdp target set <name>`
Sets active default target.

### `cdp ignore list`
Lists ignore patterns from `.cdpignore`.

### `cdp ignore add <pattern>`
Adds an rsync exclude pattern to `.cdpignore`.

### `cdp ignore remove <pattern>`
Removes an exact pattern from `.cdpignore`.

### `cdp analysis use <path>`
Sets active analysis directory (must be under project root).

### `cdp analysis tag <path>`
Tags a path inside active analysis for pull.
- default: validates path exists locally under active analysis
- `--remote`: validates path exists on remote analysis directory (for remote-only paths)

### `cdp analysis list [--remote]`
Lists subdirectories inside active analysis.
- default: local active analysis directory
- `--remote`: corresponding remote analysis directory on active target
- `--all`: include files too (not only directories)

### `cdp analysis run <command...>`
Syncs active analysis to remote, renders template, submits job.
- supports `--profile <name>` for built-in or user-defined profiles
- assigns a deterministic `run_id` from command + target + analysis + resolved resources (stored in state + job records)
- stores `pc_submit.sh`, `run.log`, and scheduler stdout/stderr under `<remote_analysis_root>/<run_id>/`

### `cdp analysis sweep run --config <yaml> [--job <name>] [--mode single|array|local] <command...>`
Submits cartesian sweep jobs from YAML `params` blocks with persisted manifests.
- command must include placeholders for sweep variables, e.g. `{lr}`, `{batch_size}`
- deterministic `run_id` per run from block name + params + command template
- manifests stored in `.cluster_dispatch/sweeps/<sweep_id>.json`
- `single`: submit each run independently
- `single` enforces a submission delay via `--submit-delay-ms` (default 5000, minimum 5000)
- `array`: submit one scheduler array job (sge/univa/pbs/slurm/lsf) using TSV mapping + wrapper script
- `local`: execute each run locally (no scheduler submission)
- supports same runtime resource override flags as `cdp analysis run`
- supports `--profile <name>` for built-in or user-defined profiles

### `cdp profile list`
Lists resource profiles. Built-ins: `small`, `long`, `highmem`.

### `cdp profile show <name>`
Shows one profile by name.

### `cdp profile set <name> [--cpus ... --memory ... --time ... --node ... --queue ... --parallel-environment ...]`
Creates or updates a user-defined profile.

### `cdp profile delete <name>`
Deletes a user-defined profile.

### `cdp doctor`
Runs preflight checks for local setup and targets.
- checks config, template validity, local binaries, active analysis path
- checks targets (scheduler, remote root, template)
- with remote checks enabled (default): SSH connectivity, remote root presence, scheduler command availability
- filters: `--target <name>`
- disable remote probes: `--no-remote`

### `cdp analysis sweep list`
Lists existing sweep manifests.

### `cdp analysis sweep show <sweep_id>`
Shows one sweep manifest and run states.

### `cdp analysis sweep resume <sweep_id>`
Submits pending runs from a sweep manifest.

### `cdp analysis sweep cancel <sweep_id>`
Cancels submitted runs from a sweep manifest.

### `cdp status`
Context-aware status (last job in active analysis context, otherwise global).
- `cdp status --job-id <id>`: query exact scheduler job id from remembered launches
- `cdp status --job-name <name>`: query exact job name from remembered launches
- `cdp status --target <name>` can be combined with `--job-id`/`--job-name`
- record-first: if a job record is already terminal (not running), `cdp status` reports that stored state without re-querying live queue
- if queue lookup returns `NOT_FOUND`, `cdp status` checks scheduler accounting and persists recovered terminal state into job history

### `cdp logs`
Shows remote log for selected job.
- default: uses last job in state
- filters: `--job-id`, `--job-name`, `--target`, `--analysis`
- views: `--head N`, `--tail N` (default tail 50), `--follow`

### `cdp cancel`
Cancels jobs using stored launch records.
- required: `--job-id` or `--job-name`
- optional filters: `--target`, `--analysis`
- scheduler-specific cancel command is used for each matched record

### `cdp stats`
Collects resource usage for a recorded job.
- required: `--job-id` or `--job-name`
- optional filters: `--target`, `--analysis`
- uses scheduler-specific accounting/stat commands on selected target
- normalizes walltime/cpu-time and memory fields into human-readable units where possible

### `cdp collect`
Collects tagged outputs for a recorded job.
- required: `--job-id` or `--job-name`
- optional filters: `--target`, `--analysis`
- uses selected record's `analysis_tags` and `remote_run_dir`

### `cdp history`
Shows remembered launch records without querying scheduler status.
- optional filters: `--target`, `--analysis`, `--job-id`, `--job-name`
- optional `--limit` (default 50)

Subcommands:
- `cdp status list [--analysis ...] [--limit ...]`
- `cdp status global [--limit ...]`

Option:
- `cdp status --target <name>` for target-scoped status from any context.

### `cdp analysis pull [--remote]`
Pulls only tagged paths for active analysis from remote run directory.
- default: pulls tagged paths that already exist locally
- `--remote`: also pulls tagged paths that are remote-only (not present locally)

## Files and state

All metadata is under project root `.cluster_dispatch/`:

- `.cluster_dispatch/config.yml`:
  targets, active analysis, analysis tags
- `.cluster_dispatch/state.json`:
  last submitted job
- `.cluster_dispatch/jobs/*.json`:
  per-run job records (all launches are preserved)
- `.cluster_dispatch/templates/scheduler_header.tmpl`:
  active scheduler template

Ignore files:
- `.gitignore`:
  Git version-control exclusions
- `.cdpignore`:
  cluster sync exclusions used by `cdp` transfers

Recommended workflow:
- Track scripts, templates, and configs with Git
- Keep large datasets outside Git
- Use `.cdpignore` to exclude heavy local content from cluster synchronization

Planned Git-aware extensions (future):
- record Git commit metadata in run records
- `cdp git status`
- reproducibility guardrails such as `--require-git-clean`

## Example template (PBS Pro)

```bash
#PBS -N {job_name}
#PBS -o {stdout}.out
#PBS -e {stderr}.err
#PBS -l select={node}:ncpus={cpus}:mem={memory}
#PBS -l walltime={time}
#PBS -q {queue}
```

## Troubleshooting

### `No active analysis set`
Run:

```bash
cdp analysis use <path>
```

### `Template missing required placeholders`
Ensure your template includes:
- `{cpus}`, `{memory}`, `{time}`, `{job_name}`, `{stdout}`, `{stderr}`, `{working_dir}`

### `--queue is required ...`
Your template references `{queue}` but neither:
- `cdp analysis run --queue ...`, nor
- target `default_queue`
is set.

### `Target '<name>' not found`
Run:

```bash
cdp target list
```

and set one:

```bash
cdp target set <name>
```

### `cdp analysis pull` says no tags found
Tag paths first:

```bash
cdp analysis tag <path-inside-active-analysis>
```

## Security notes

- This tool executes remote commands via SSH.
- Review templates and commands before running on shared systems.
- Prefer least-privilege SSH keys and explicit host aliases.

## Roadmap ideas

- required-file preflight checks before run
- richer status filtering (`--job-id`, `--state`)
- retention policies for `.cluster_dispatch/jobs`
- optional dry-run mode for sync and submit

## Contributing

Issues and PRs are welcome.

If you propose behavior changes, include:
- expected CLI UX,
- compatibility impact,
- migration behavior for existing config/state.
