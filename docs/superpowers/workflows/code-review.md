# Code Review & PR Workflow

## Before opening a PR

### 1. Run verification skill

```
/verification-before-completion
```

This must pass: `make lint`, `make test`, `make build`.

### 2. Request code review

```
/requesting-code-review
```

Follow the skill's output. Fix any issues it surfaces.

### 3. Open the PR

```bash
gh pr create \
  --title "feat: short description under 70 chars" \
  --body "$(cat <<'EOF'
## Summary
- What this PR does (2-3 bullets)

## Test plan
- [ ] make lint passes
- [ ] make test passes
- [ ] make build passes
- [ ] Manually tested with make run + curl

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

## Reviewing a PR

```
/review
```

Or use the Serena MCP to navigate changed symbols:

```
Use mcp__serena__find_referencing_symbols on any changed interface to verify all callers were updated
```
