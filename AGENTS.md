This file applies to the entire repository.

hserver is a configurable HTTP server adaptor built on top of `hyper` and `tower`, with explicit accept-loop control and optional Rustls support.

## Global Working Rules

- English is the repository's written language: documentation, code, and comments always use it. User-facing UI text in another language is deliberate and stays untranslated.
- Read `README.md` before starting non-trivial work. Treat `README.md` as the source of truth for the whole system's design and architecture. Always follow a documentation-first approach when making any modifications, ensuring all documentation is updated alongside every change.
- Be strict. Avoid fuzzy, ambiguous, or weakly specified behavior; make semantics explicit.
- Name booleans with predicate-style name prefixes such as `is`, `can`, `has`, or `should`.
- Prefer unabbreviated names. For example, use `permanent_id` instead of `perm_id`. Exceptions: "ctx", "txn", "err", "repr", "src", "dst", "std", "id".
- Give every item the most restrictive visibility that permits its intended use.
- Add comments for non-trivial or hard-to-understand logic.
- Every `AGENTS.md` must have a sibling `CLAUDE.md` containing exactly `@AGENTS.md`.
- Follow test-driven development (TDD): write tests before implementing functionality.
- Do not write trivial tests. Tests exist to protect complex logic, invariants, edge cases, and failure-prone paths, never to satisfy a ritual or verify self-evident modifications (such as mechanically asserting the presence or absence of a newly added or removed field). Keep coverage cohesive rather than inflating test counts: when functionality changes, update and consolidate existing tests where appropriate instead of accumulating redundant cases. Test only first-party domain logic; never test upstream dependencies, libraries, or standard-library primitives (such as string formatting or basic macro expansion) unless explicitly relying on fragile, unspecified external behavior. In TDD, apply the same restraint: write tests first only when designing non-trivial logic, not for boilerplate or compiler-guaranteed mechanics.
- After every code modification, run `cargo fmt`, `cargo check`, `cargo clippy`, and `cargo test` to ensure no formatting issues, compilation errors, lint regressions, or test failures were introduced.
- Treat fatal configuration, contract and invariant violations as fatal: use `unwrap()` / `expect()` instead of adding recovery paths that obscure bugs and complicate debugging.
- Do not contort idiomatic code into verbose workarounds merely to dodge panic-related Clippy lints (such as `unwrap_used`, `expect_used`, `panic`, `indexing_slicing`, `todo`, `unimplemented`, or `unreachable`). When panicking is safe and acceptable—such as a proven invariant or an intentional fail-fast fatal condition—retain the direct construct and document the justification using `#[expect(..., reason = "...")]` on the narrowest applicable scope (such as a single statement or match arm, rather than a whole function or block).
- Rustdoc is mandatory: every item, private ones included, and the crate itself must be documented, intra-doc links must resolve, and `# Panics` and `# Safety` sections are required where the corresponding Clippy lints apply. Lint policy is configured centrally in `[lints]` in the root `Cargo.toml` and in `clippy.toml`; never override it per module.
