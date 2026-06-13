# Instructions

## Ground Rules

### Replies

**ALWAYS** start replies with STARTER_CHARACTER + space (default: 🍀). Stack emojis when requested, don't replace.
STARTER_CHARACTER =❗️when flagging an error or my misconception, 🌀 when refactoring, ♻️ when rules just re-read, always followed by a space.
#### Style

- Tell me something I need to know even if I don't want to hear it
- Don't assume, try to back up your response with documentation/guides/articles/blogs
- Exemplary,  include examples and tutorials for common use cases. 
- Write in a markdown syntax that can be used in Obsidian.
- Write less, be minimal where possible.
- Only include what genuinely helps the reader. Avoid verbose text that buries the essential info.
- Outline first; refine later.
- Structure your docs with a high-level outline before deep writing.
- Use clear language, plain structure, headings, concise sentences - editing is essential. Help user to understand the information at a glance.
- Skimmable, readers should scan for answers quickly. 

## Project
This is a project to create an alternative to Terraform for SQL databased without using a state. 

The goal is to create and GitHub action written in python that could be triggered via a workflow.

### Project Structure

```
sqliac/
├── src/
│   └── sqliac/
│       ├── __init__.py          # Main package init
│       ├── cli.py               # Command line usage
│       ├── main.py
│       ├── configuration_manager.py
│       ├── drift.py
│       ├── execution_plan.py
│       ├── scheduler.py
│       ├── template_engine.py
│       ├── value_sanitizer.py
│       ├── adapters/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── factory.py
│       │   ├── snowflake_adapter.py
│       │   └── sqlite.py
│       └── resources/
│           ├── __init__.py
│           ├── resources_schema.py
│           └── resources.toml
├── tests/
│   ├── __init__.py
│   ├── test_configuration_manager.py
│   ├── test_execution_plan.py
│   ├── test_scheduler.py
│   ├── test_template_engine.py
│   ├── test_value_sanitizer.py
│   ├── test_adapters_base.py
│   └── test_drift.py
├── pyproject.toml
├── README.md
├── LICENSE
└── .github/
    └── workflows/
```

## Coding
### Purpose

This project follows the principles of _A Philosophy of Software Design_ (John Ousterhout).  
The primary goal working on this codebase is to **minimize complexity over time**.

Complexity is treated as the main source of bugs, slow development, and system fragility.  
All design and implementation decisions should prioritize long-term simplicity, clarity, and ease of change.

---

### Core Principles

#### 1. Complexity Is the Enemy

Complexity is anything that:

- Makes code harder to understand
    
- Causes unexpected side effects
    
- Requires understanding multiple unrelated components to make a change
    

Agents must actively reduce:

- **Change amplification** (small changes touching many places)
    
- **Cognitive load** (too much to hold in mind at once)
    
- **Unknown unknowns** (unclear consequences of changes)
    

If a change feels risky or surprising, complexity is leaking.

---

#### 2. Prefer Strategic Over Tactical Changes

Agents should practice **strategic programming**:

- Invest time in design to reduce future complexity
    
- Prefer small redesigns over quick fixes
    
- Refactor early when complexity appears
    

Short-term convenience must not increase long-term maintenance cost.

---

#### 3. Design Deep Modules

A module should:

- Hide substantial complexity internally
    
- Expose a **small, simple, and obvious interface**
    
- Be easy to use correctly and hard to misuse
    

Avoid shallow abstractions that:

- Merely wrap existing logic
    
- Expose internal details
    
- Add indirection without simplification
    

If the interface feels busy or fragile, redesign the module.

---

#### 4. Enforce Information Hiding

Each module owns its decisions and implementation details.

Agents must hide:

- Data structures
    
- Internal workflows
    
- Performance optimizations
    
- Error-handling strategies
    
- Caching, retries, or fallback logic
    
- Third-party APIs and integration details
    

No internal concept should leak across module boundaries.

---

#### 5. Keep Interfaces Minimal and Obvious

Good interfaces:

- Are small and consistent
    
- Avoid flags, mode switches, and optional behavior
    
- Require no knowledge of implementation details
    
- Do not expose special cases
    

If correct usage requires reading the implementation, the interface is incorrect.

---

#### 6. Pull Complexity Downward

Complexity should live in **one place**, not be duplicated across callers.

Agents should:

- Move shared logic into the callee
    
- Centralize validation, retries, formatting, and edge-case handling
    
- Eliminate repeated patterns across call sites
    

If multiple callers must behave carefully, the abstraction is wrong.

---

#### 7. Handle Edge Cases at the Lowest Level

Rare cases and errors are major sources of complexity.

Agents must:

- Design edge cases into the abstraction
    
- Avoid pushing special-case logic to callers
    
- Ensure exceptional behavior does not complicate normal usage
    

Do not let “rare paths” leak complexity into common paths.

---

#### 8. Naming Is a Design Responsibility

Names are compressed documentation.

Good names:

- Describe intent, not implementation
    
- Match the abstraction level
    
- Avoid vague terms like `Manager`, `Helper`, or `Utils`
    

If a name needs explanation, it is incorrect.

---

#### 9. Comments Explain _Why_, Not _What_

Comments should:

- Explain intent, constraints, and tradeoffs
    
- Capture non-obvious reasoning
    
- Document design decisions
    

Comments must not:

- Restate the code
    
- Compensate for poor abstractions
    
- Explain obvious mechanics
    

If comments are needed to understand how to use an API, redesign the API.

---

#### 10. Treat Design as Continuous Work

Design is not a phase; it is ongoing.

Agents are expected to:

- Refactor when complexity emerges
    
- Redesign interfaces that begin to leak
    
- Treat confusion as a bug
    

Do not defer cleanup with the intention to “fix it later.”

---

### Agent Decision Heuristics

When making changes, agents should ask:

- Where should this complexity live?
    
- Can this interface be smaller?
    
- Will this surprise a future reader?
    
- Does this reduce or increase cognitive load?
    
- Can multiple concerns be separated more cleanly?
    

If unsure, favor the design that:

- Reduces future decision-making
    
- Minimizes cross-module knowledge
    
- Makes the system easier to reason about
    

---

### Review and Enforcement

Changes should be rejected if they:

- Increase change amplification
    
- Add shallow abstractions
    
- Leak internal details
    
- Require comments to explain usage
    
- Duplicate logic across callers
    

Complexity reduction is considered a valid and valuable change, even without new functionality.

---

### Guiding Rules

> **If the system becomes easier to understand and change, the design is improving.  
> If not, complexity is being added and must be addressed.**

---

## Project-specific Guidance

- The Python package lives under `src/sqliac` and is executed via `python -m sqliac` or `python -m sqliac.cli`.
- Primary CLI commands are `graph`, `list`, `init`, and `compile`.
- `compile` expects a target of the form `<provider>.<resource>` and uses `--ddl` or `--state` plus `--mode` for DDL.
- Provider templates and resource definitions are split between `providers/` and `definitions/`.
- The GitHub Action is defined in `action.yml` and builds a Docker image from `Dockerfile`; keep changes compatible with container usage.
- Code formatting and linting follow `ruff` rules in `pyproject.toml` and `black` conventions from `requirements.txt`.
- Prefer preserving the no-state SQL infrastructure design; avoid adding hidden state or provider-specific side effects.
- If you add tests, use `pytest` and keep test files in the workspace root or a `tests/` directory consistent with existing naming.
- Avoid duplicating provider handling logic across `providers_loader.py`, `definitions_loader.py`, `execution_plan.py`, and `template_engine.py`.
- When uncertain, prefer small, obvious interfaces and move shared validation into the lower-level module.

