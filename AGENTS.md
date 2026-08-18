# Orchestrator Repository Agent Entry

## Common authority routing

- This file is a repository entry point, not a copy of team policy.
- Cross-repository policy, roles, Wave/WBS state, assignments, Git/PR workflow, and shared safety rules are owned only by the sibling `../urizo-final-master/AGENTS.md` and its required current-status documents.
- Before planning or editing, read that Master authority from the canonical parent workspace. If the sibling Master checkout is unavailable, do not infer current work from this repository alone; reopen the canonical four-repository workspace or synchronize Master first.
- Claude Code uses `CLAUDE.md`, which imports this file. Do not add a second copy of common policy there.

## Repository-local scope

- Own only the Python LangGraph Coding Runtime, its Backend contract consumers, checkpoint/interrupt/resume behavior, tests, image, and dependency lock.
- Spring owns the platform API, batch, core authorization, Provider/model authority, Tool execution, and Core persistence boundary.
- Keep Python/runtime pins, commands, and repository-local verification in `README.md`, `pyproject.toml`, and `uv.lock` rather than duplicating them in agent policy.
