# Adding `read_artifacts` to `template_ctx`

## Problem

Some languages require files to be compiled in dependency order, where dependencies are declared within the source files themselves. For example, Lean modules must be compiled in a topological order declared by the include statements in the files; you must first compile all modules included by a Lean module before compiling the module itself. Typically, the Lean build system, Lake, handles this by compiling in 2 phases:

1. Quickly parse the includes to generate a dependency graph, then
2. compile the Lean modules in a topological ordering.

This presents an issue in Bazel, typically the action graph is fixed at analysis time before the source files can be read.

## Solution

This is a proof-of-concept demonstrating `template_ctx.read_artifacts()`, a proposed API addition to `map_directory`. 
It enables templates to read dependency manifests and resolve exec paths to `File` objects
This bridges the gap between exec-time dependency parsing tools and Starlark orchestration.

## Solution constraints

1. **Closed dependency graph**: Actions cannot have inputs outside `bazel query 'deps(<target>)'`
2. **No action conflicts**: Dynamic actions cannot create conflicting outputs
3. **Starlark stays lightweight**: Expensive computation (parsing source files) belongs in compiled actions, not Starlark

## `template_ctx.read_artifacts(...)`

```python
template_ctx.read_artifacts(file) -> list[File]
```

**Parameters:**
- `file`: A `File` artifact that is a known input to this template expansion

**Returns:**
- A list of `File` objects corresponding to the paths listed in the manifest file
- Each path in the manifest is resolved to an artifact that `template_ctx` is aware of

**Behavior:**
- Bazel reads the file contents and parses it as a list of file paths
- Each path is resolved to a known artifact
- If any path cannot be resolved to a known artifact, an error is raised

### Manifest Format

The manifest file format needs more consideration.
`read_artifacts()` resolves paths to existing inputs only, not outputs being declared.
This demo uses a manifest format containing exec paths of source files:

```
src/lib/helper.src
src/lib/utils.src
bazel-out/k8-fastbuild/bin/src/gen/generated.src
```

For checked-in source files, the exec path equals the workspace-relative path. 
For generated files (outputs from previous actions), the exec path includes the `bazel-out/...` prefix.

The template maps these resolved source `File` objects to their declared outputs to establish dependencies.

## Demo Flow

This demo uses a simple toy language (`.src` files) with import statements:

```
import src/lib/utils.src
import src/gen/generated.src
print("hello")
```

A Go compiler (`srcc`) handles both parsing and compilation. The demo includes both checked-in source files and a generated file (`src/gen/generated.src`) to demonstrate exec path handling.

**Phase 1 (Resolver):** `srcc resolve` parses source files for imports and writes manifests. It maps logical import paths to exec paths—generated files get their full `bazel-out/...` path.

**Phase 2 (map_directory):** The template reads manifests via `read_artifacts()`, which resolves exec paths to `File` objects. It then maps each source to its declared output to establish the correct build order.

## Running the Demo

First, build Bazel from source with the `read_artifacts()` implementation:

```bash
# From the bazel repository root
bazel build //src:bazel
```

Then use the built Bazel to run the demo:

```bash
cd examples/map_directory_read_artifacts
../../bazel-bin/src/bazel build //:myapp
```
