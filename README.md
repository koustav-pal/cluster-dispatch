# project-control

`project-control` is a Python CLI for running analysis jobs on remote SSH compute targets (SGE, Univa, PBS Pro, Slurm, LSF, or no scheduler), while keeping your local analysis directory as the source of truth.

It is designed for workflows where you:
- keep multiple analyses under one project,
- run jobs across multiple clusters/servers,
- track job status from your laptop,
- pull only tagged outputs back to local.

## Why this exists

HPC workflows often become hard to manage when commands, job scripts, paths, and output pulls are done ad hoc across many servers.

`project-control` gives you a repeatable interface:
- one project-level config,
- explicit target definitions,
- analysis-level tagging for pull,
- scheduler template rendering with runtime/default resources.

## Core concepts

- `Project root`: directory where `pc init` is run.
- `Target`: named remote execution profile (host + scheduler + remote root + default resources + template).
- `Active analysis`: local directory selected by `pc analysis use`.
- `Tagged paths`: paths inside active analysis selected by `pc analysis tag`; `pc analysis pull` only pulls these.
- `Remote analysis root`: `<remote_root>/<active_analysis_path>`

No timestamped run directories are used. Remote structure mirrors local analysis path.

## Current status behavior

`pc status` is context-aware:
- inside active analysis directory: shows latest job status,
- outside active analysis directory: shows global status across targets.

You can always force target-specific status:
- `pc status --target <target_name>`

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
pc --help
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
pc init \
  --host cluster-a \
  --scheduler sge \
  --remote-root /scratch/me/projects \
  --template-file ./my_sge_template.tmpl
```

What this does:
- creates `.project_control/config.yml`
- creates local state dirs under `.project_control/`
- ensures remote project root exists:
  `ssh <host> mkdir -p <remote_root>`
- creates default target in config

If `--target` is omitted, it defaults to your current directory name.

### 3. Set analysis and tags

```bash
pc analysis use analyses/run_001
pc analysis tag results
pc analysis tag reports/figures
pc analysis tag --remote remote_only_outputs
pc analysis list
pc analysis list --all
pc analysis list --remote
pc analysis list --remote --all
```

### 4. Run job

Using target defaults only:

```bash
pc analysis run python train.py --epochs 20
```

Override resources at runtime:

```bash
pc analysis run \
  --cpus 8 \
  --memory 32G \
  --time 04:00:00 \
  --queue normal \
  --parallel-environment smp \
  --job-name run001 \
  python train.py --epochs 20
```

### 5. Monitor and pull

```bash
pc status
pc status --job-id 123456
pc status --job-name run001
pc status --target cluster-a
pc history
pc history --analysis analyses/run_001 --limit 20
pc logs
pc logs --job-id 123456 --tail 100
pc logs --job-name run001 --head 50
pc logs --follow
pc cancel --job-id 123456
pc cancel --job-name run001 --target cluster-a
pc collect --job-id 123456
pc collect --job-name run001
pc status list --analysis analyses/run_001
pc status global
pc analysis pull
```

## Multi-target workflow

Add another target:

```bash
pc target add cluster-b \
  --host cluster-b \
  --scheduler pbs \
  --remote-root /scratch/me/projects
```

Switch active target:

```bash
pc target set cluster-b
```

List targets:

```bash
pc target list
```

## Resource resolution order

For each run resource (`cpus`, `memory`, `time`, `node`, `queue`, `parallel_environment`):
1. runtime flag (if provided),
2. target default from config.

If template contains `{queue}` or `{parallel_environment}`, those values must resolve non-empty (runtime or default), otherwise run fails fast.

## Command reference

### `pc init`
Initializes project-control in current directory.

Key options:
- `--host` (required)
- `--scheduler` (required: `sge|univa|pbs|slurm|lsf|none`)
- `--remote-root` (required, absolute remote path)
- `--template-file` (required)
- `--target` (optional)
- default resource options:
  - `--cpus`
  - `--memory`
  - `--time`
  - `--node`
  - `--queue`
  - `--parallel-environment`

### `pc target add <name>`
Adds/updates a target by name.

Key options:
- `--host`
- `--scheduler`
- `--remote-root`
- `--template-file`
- default resource options (same as init)

### `pc target set <name>`
Sets active default target.

### `pc analysis use <path>`
Sets active analysis directory (must be under project root).

### `pc analysis tag <path>`
Tags a path inside active analysis for pull.
- default: validates path exists locally under active analysis
- `--remote`: validates path exists on remote analysis directory (for remote-only paths)

### `pc analysis list [--remote]`
Lists subdirectories inside active analysis.
- default: local active analysis directory
- `--remote`: corresponding remote analysis directory on active target
- `--all`: include files too (not only directories)

### `pc analysis run <command...>`
Syncs active analysis to remote, renders template, submits job.

### `pc status`
Context-aware status (last job in active analysis context, otherwise global).
- `pc status --job-id <id>`: query exact scheduler job id from remembered launches
- `pc status --job-name <name>`: query exact job name from remembered launches
- `pc status --target <name>` can be combined with `--job-id`/`--job-name`
- record-first: if a job record is already terminal (not running), `pc status` reports that stored state without re-querying live queue
- if queue lookup returns `NOT_FOUND`, `pc status` checks scheduler accounting and persists recovered terminal state into job history

### `pc logs`
Shows remote log for selected job.
- default: uses last job in state
- filters: `--job-id`, `--job-name`, `--target`, `--analysis`
- views: `--head N`, `--tail N` (default tail 50), `--follow`

### `pc cancel`
Cancels jobs using stored launch records.
- required: `--job-id` or `--job-name`
- optional filters: `--target`, `--analysis`
- scheduler-specific cancel command is used for each matched record

### `pc collect`
Collects tagged outputs for a recorded job.
- required: `--job-id` or `--job-name`
- optional filters: `--target`, `--analysis`
- uses selected record's `analysis_tags` and `remote_run_dir`

### `pc history`
Shows remembered launch records without querying scheduler status.
- optional filters: `--target`, `--analysis`, `--job-id`, `--job-name`
- optional `--limit` (default 50)

Subcommands:
- `pc status list [--analysis ...] [--limit ...]`
- `pc status global [--limit ...]`

Option:
- `pc status --target <name>` for target-scoped status from any context.

### `pc analysis pull [--remote]`
Pulls only tagged paths for active analysis from remote run directory.
- default: pulls tagged paths that already exist locally
- `--remote`: also pulls tagged paths that are remote-only (not present locally)

## Files and state

All metadata is under project root `.project_control/`:

- `.project_control/config.yml`:
  targets, active analysis, analysis tags
- `.project_control/state.json`:
  last submitted job
- `.project_control/jobs/*.json`:
  per-run job records (all launches are preserved)
- `.project_control/templates/scheduler_header.tmpl`:
  active scheduler template

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
pc analysis use <path>
```

### `Template missing required placeholders`
Ensure your template includes:
- `{cpus}`, `{memory}`, `{time}`, `{job_name}`, `{stdout}`, `{stderr}`, `{working_dir}`

### `--queue is required ...`
Your template references `{queue}` but neither:
- `pc analysis run --queue ...`, nor
- target `default_queue`
is set.

### `Target '<name>' not found`
Run:

```bash
pc target list
```

and set one:

```bash
pc target set <name>
```

### `pc analysis pull` says no tags found
Tag paths first:

```bash
pc analysis tag <path-inside-active-analysis>
```

## Security notes

- This tool executes remote commands via SSH.
- Review templates and commands before running on shared systems.
- Prefer least-privilege SSH keys and explicit host aliases.

## Roadmap ideas

- required-file preflight checks before run
- richer status filtering (`--job-id`, `--state`)
- retention policies for `.project_control/jobs`
- optional dry-run mode for sync and submit

## Contributing

Issues and PRs are welcome.

If you propose behavior changes, include:
- expected CLI UX,
- compatibility impact,
- migration behavior for existing config/state.
