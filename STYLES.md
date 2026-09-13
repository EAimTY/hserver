This file contains the coding style guide that applies to the whole project. Periodical style-fixing agent runs should be done against this guide.

- Never add source newlines to string literals, templates, or document files, and never wrap their content, solely to limit line length; preserve only semantically intentional line breaks.
- Write comment prose as one line per paragraph and let `cargo fmt` wrap it at the configured `comment_width`; never hand-wrap comments.
- Import every out-of-module type, including structs, enums, type aliases, and traits, and refer to it only by its type name, never by a path.
- Refer to constants, statics, and free functions through exactly one meaningful module qualifier: `crate::item`, `super::item`, or `module::item`. Except for items defined in the caller's module, never import the value item itself or call it unqualified. Import and alias its module when needed; if the module name does not describe the item clearly, choose a clearer alias. Do not use a longer inline path at the call site.
- In import paths, prefix direct child modules with `self::`, except that the crate root uses `crate::`. Use `super::` for sibling or ancestor modules.
- A child module must not import its parent's private imports. When the parent re-exports an item, even with `pub(self)`, prefer importing that re-export from the parent.
- Use the prelude `Result` from `std::result`, not specialized aliases such as `std::fmt::Result`.
- Import external error types as `XxxError`. Avoid paths like `serde_json::Error` in type positions.
- When a trait is imported only to make its methods or associated items available, and is not named in an implementation, trait bound, trait object, or type position, import it with `as _`.
- Never constrain a type parameter inside the generic parameter list; always write bounds in a `where` clause. Write `fn f<T>(x: T) where T: Clone`, not `fn f<T: Clone>(x: T)`; the same applies to `struct`, `enum`, `impl`, and `trait` declarations. Prefer `impl Trait` (argument or return position) over a named generic whenever the parameter does not need to be named elsewhere in the signature or body.
- Do not put additional trait bounds on trait definitions or associated types. Keep them at each use site beside the defining trait's bound; for example, use `T: Provider + Send + Sync` and `T::Error: Send` where needed. The only exceptions are `Sized` and `?Sized`.
- Generic parameter naming is all-or-nothing per list. When a generic list is simple — a few distinct parameters with no overlapping roles (for example no two closures, no `Future`) — use single-letter generics. Otherwise, once any parameter needs a descriptive multi-letter name, make every parameter in that list multi-letter; never mix single-letter and multi-letter generics in the same list. Const generics are exempt.
- Use role-based names for multi-letter generics and closures: prefix type parameters with `T` and function/closure parameters with `F`, followed by a description of their use (for example `TStream`, `TUpdate`, `FSubscribe`); name fields and parameters that hold a closure with a descriptive `<role>_fn` form (for example `subscribe_fn`, `apply_update_fn`) rather than a vague name such as `map`, `open`, or `apply`.
- Separate a multi-line statement from every adjacent statement with a blank line. Single-line statements may be adjacent only when they are closely related siblings. Separate large code blocks, including match arms with substantial bodies, with blank lines.
- Order items in a Rust file as follows. Within a category marked public first, order visibility as `pub`, `pub(crate)`, `pub(super)` / `pub(in path)`, then private.
    1. Private imports.
    2. Child modules.
    3. Public re-exports. If no child module separates imports from re-exports, annotate the re-exports with `#[rustfmt::skip]` so rustfmt cannot merge the two groups.
    4. Statics and constants, public first.
    5. Types, including structs, enums, type aliases, and traits, public first.
    6. A public standalone function that serves as the module's main entry point, when one exists.
    7. Inherent `impl Type` blocks, significant before trivial blocks containing only simple constructors, setters, or getters. Put public items first within each block.
    8. Non-trivial trait implementations that contain actual logic rather than simple wrappers.
    9. Standalone functions, public first and trivial last.
    10. Trivial trait implementations, such as wrappers, or `Clone`.
