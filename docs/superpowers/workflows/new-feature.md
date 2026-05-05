# New Feature Workflow

This is the standard lifecycle for any feature in this repo.

## 1. Brainstorm

```
/brainstorming
```

Answer the skill's questions. Output: approved design in `docs/superpowers/specs/`.

## 2. Write a Plan

The brainstorming skill invokes this automatically:

```
/writing-plans
```

Output: implementation plan in `docs/superpowers/plans/`.

## 3. Implement with TDD

Before writing implementation code, invoke:

```
/test-driven-development
```

Follow the plan task by task. Each task:
1. Write the failing test
2. Run it — confirm it fails
3. Write minimal implementation
4. Run it — confirm it passes
5. Commit

## 4. Verify Before Committing

Before the final commit or PR:

```
/verification-before-completion
```

This runs `make lint`, `make test`, `make build` and confirms all pass.

## 5. Open a PR

See `docs/superpowers/workflows/code-review.md`.
