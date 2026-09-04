# Submitting Issues

We use GitHub issues to track public bugs. If you are submitting a bug, please provide
as much info as possible to make it easier to reproduce the issue and update unit tests.

# Contributing Code

This project uses [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/#summary).

We follow the [GitHub Flow](https://guides.github.com/introduction/flow/index.html) so all contributions happen through pull requests. We actively welcome your pull requests:

1.  Fork the repo and create your branch from master.
2.  If you've added code that should be tested, add tests.
3.  If you've changed APIs, update the documentation.
4.  Ensure the test suite passes.
5.  Issue that pull request!

When submitting code changes your submissions are understood to be under the same MIT License that covers the project. Feel free to contact Eyevinn Technology if that's a concern.

# TypeScript migration

The `engine/` tree is being migrated from JavaScript to TypeScript **incrementally**, one
module at a time, so that `master` keeps building and testing green throughout. The build
(`npm run build`, which runs `tsc --project ./`) already compiles a mixed JS/TS tree because
`allowJs` is enabled in `tsconfig.json`.

## Target strictness level

The end state is a fully-typed, strictly-checked tree:

- `strict: true` (implies `noImplicitAny`, `strictNullChecks`, etc.)
- `checkJs: true` for any remaining `.js` files

These global flags are **intentionally not enabled yet**. Turning them on across the ~7,500
LOC of currently-untyped JS would surface a large number of errors at once and break
`master`'s build. Instead, opt in **per file** as each module is hardened:

- Add `// @ts-check` at the top of a `.js` file to type-check just that file, or
- Convert the file to `.ts` (preferred — see checklist below).

The global `strict` / `checkJs` flags land **last**, in the final migration slice (**#376**),
once every module has already been converted or individually `@ts-check`-clean.

## Per-file conversion checklist

Convert **one module per commit** to keep diffs reviewable and bisectable:

1. Rename the file `.js` → `.ts` (use `git mv` to preserve history).
2. Add explicit types for all exported functions, classes, and their public members.
3. Keep the **public API and runtime behavior identical** — this is a type-only migration,
   no behavior changes. Prefer `unknown` + narrowing over `any` where practical; leave a
   `// TODO(ts):` note if a proper type is deferred.
4. Run `npm run build && npm test` locally and confirm both are green (the jasmine suite must
   report `0 failures`).
5. Commit with a conventional-commit message, e.g. `refactor(engine): migrate util to TypeScript`.

CI already gates every push on `npm run build` (tsc) followed by `npm test`, so a file that
fails type-checking or breaks a test cannot land on `master`.

# Code of Conduct

## Our Pledge

In the interest of fostering an open and welcoming environment, we as contributors and maintainers pledge to making participation in our project and our community a harassment-free experience for everyone, regardless of age, body size, disability, ethnicity, gender identity and expression, level of experience, nationality, personal appearance, race, religion, or sexual identity and orientation.

## Our Standards

Examples of behavior that contributes to creating a positive environment include:

- Using welcoming and inclusive language
- Being respectful of differing viewpoints and experiences
- Gracefully accepting constructive criticism
- Focusing on what is best for the community
- Showing empathy towards other community members

Examples of unacceptable behavior by participants include:

- The use of sexualized language or imagery and unwelcome sexual attention or advances
- Trolling, insulting/derogatory comments, and personal or political attacks
- Public or private harassment
- Publishing others' private information, such as a physical or electronic address, without explicit permission
- Other conduct which could reasonably be considered inappropriate in a professional setting

## Our Responsibilities

Project maintainers are responsible for clarifying the standards of acceptable behavior and are expected to take appropriate and fair corrective action in response to any instances of unacceptable behavior.

Project maintainers have the right and responsibility to remove, edit, or reject comments, commits, code, wiki edits, issues, and other contributions that are not aligned to this Code of Conduct, or to ban temporarily or permanently any contributor for other behaviors that they deem inappropriate, threatening, offensive, or harmful.

## Scope

This Code of Conduct applies both within project spaces and in public spaces when an individual is representing the project or its community. Examples of representing a project or community include using an official project e-mail address, posting via an official social media account, or acting as an appointed representative at an online or offline event. Representation of a project may be further defined and clarified by project maintainers.

## Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be reported by contacting the project team. All complaints will be reviewed and investigated and will result in a response that is deemed necessary and appropriate to the circumstances. The project team is obligated to maintain confidentiality with regard to the reporter of an incident. Further details of specific enforcement policies may be posted separately.

Project maintainers who do not follow or enforce the Code of Conduct in good faith may face temporary or permanent repercussions as determined by other members of the project's leadership.

## Attribution

This Code of Conduct is adapted from the Contributor Covenant, version 1.4, available at http://contributor-covenant.org/version/1/4
