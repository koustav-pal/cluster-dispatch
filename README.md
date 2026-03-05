# project-control

CLI tool to initialize, run, monitor, and pull analysis jobs on remote SSH compute targets (SGE, PBS, or no scheduler).

All Project Control metadata is stored under `.project_control/` at the project root.

## Install

```bash
pip install -e .
```

## Quickstart

```bash
pc init --project-id my-project --host cluster-a --scheduler sge --remote-root /scratch/me/projects --template-file ./my_sge_template.tmpl
# Add another target later:
pc target add cluster-b --host cluster-b --scheduler pbs --remote-root /scratch/me/projects
# Update existing target paths/scheduler later without repeating host:
pc target add cluster-a --scheduler sge
pc analysis use analyses/run_001
pc analysis tag results/figures
pc run python train.py --epochs 20
pc status
pc status --target cluster-a
pc status global
pc status list --analysis analyses/run_001
pc pull
```

## Notes

- Requires `ssh` and `rsync` on your machine.
- Use SSH aliases in `~/.ssh/config` for stable host definitions.
- Optional `.pcignore` in project root is passed to `rsync --exclude-from`.
- Main config file is `.project_control/config.yml`.
- Scheduler template is user-provided during init and stored at `.project_control/templates/scheduler_header.tmpl`.
- `pc init` also creates the default target using provided `--host`, `--scheduler`, and `--remote-root`.
- If `--default-target` is omitted, it defaults to the current project root directory name.
- During `pc init`, Project Control runs `ssh <host> mkdir -p <remote-root>/<project_id>` to ensure the remote project root exists.
- Required template placeholders: `{cpus}`, `{memory}`, `{time}`, `{job_name}`, `{stdout}`, `{stderr}`.
- Optional template placeholders: `{queue}`, `{node}`, `{parallel_environment}`.
- Each target can define default resources (`cpus`, `memory`, `time`, `node`, `queue`, `parallel_environment`) via `pc init` or `pc target add`.
- `pc run` uses target defaults for any resource flags not provided at runtime.
- `--job-name` is optional; if omitted, a random hash-based name is generated.
- `{stdout}` and `{stderr}` template variables are populated automatically and default to the resolved `job_name`.
- If template includes `{queue}`, queue must be provided either at runtime (`--queue`) or via target default.
- If template includes `{parallel_environment}`, it must be provided either at runtime or via target default.
- `pc analysis use` requires an explicit analysis directory path.
- `pc analysis tag <dir>` tags any folder inside the active analysis directory for future pulls.
- `remote_root` is an absolute path on the target where project runs are created.
- Remote run paths mirror the active analysis path exactly under `<remote_root>/<project_id>/` (no timestamped run folders).
- `pc pull` only pulls paths that were tagged via `pc analysis tag`.
- `pc status list` lists recent job IDs/states for an analysis (defaults to active analysis).
- `pc status` is context-aware: inside active analysis it shows last-job status; outside it shows global cross-target status.
- `pc status --target <name>` shows scheduler status for that target regardless of current directory context.
