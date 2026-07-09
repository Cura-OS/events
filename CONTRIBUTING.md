# Contributing to events

Thanks for your interest in contributing to CuraOS!

## Ground Rules

By participating, you agree to abide by the [Code of Conduct](./CODE_OF_CONDUCT.md).

## How to Contribute

1. **Fork** the repository
2. **Clone** your fork locally
3. **Create a branch**: `git checkout -b feat/your-feature`
4. **Make changes** following our code style
5. **Test**: run `bun test` and ensure all tests pass
6. **Commit**: use [Conventional Commits](https://www.conventionalcommits.org/)
7. **Push** to your fork
8. **Open a Pull Request**

## Development Setup

```bash
git clone <your-fork>
cd events
bun install
bun test
```

## Code Style

- TypeScript with strict mode
- kebab-case file names, PascalCase classes (no I-prefix)
- Co-located `*.spec.ts` for unit tests, `*.e2e-spec.ts` for E2E
- WHY-not-WHAT comments + TSDoc on public exports
- No AI/tool attribution in commits (no Co-authored-by AI trailers)

## Commit Convention

Use Conventional Commits:

```
feat: add new feature
fix: resolve a bug
docs: update documentation
refactor: code restructure
```

## License

By contributing, you agree that your contributions will be licensed under the same license as this project (LicenseRef-CuraOS-BSL).
