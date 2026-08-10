# Orchestrator Repository Rules

- Scope: Python LangGraph coding-agent runtime and its integration contracts.
- Follow the parent workspace architecture and Git workflow rules.
- Spring owns the platform API, batch, core authorization, and persistence boundary.
- Do not create product scaffolding until the Stage 0 implementation start is explicitly approved.
- Pin the future Python version and dependencies in repository-managed lock files.
- Keep secrets out of source, prompts, logs, commits, and pull requests.
- Normal work branches from the latest `dev` and reaches `dev` through a reviewed pull request.

