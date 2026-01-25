"""Rule for compiling .src files with dynamic dependency resolution.

This demonstrates the read_artifacts() API in map_directory. The pattern is:

1. Resolver action: Parses source files for "import" statements and writes
   manifest files. Each manifest contains exec paths of:
   - The source files that this file imports (for intra-build dependencies)
   - These are used to determine build ordering

2. map_directory: Uses read_artifacts() to resolve manifest paths to File objects,
   then creates compile actions with correct dependency ordering.

This keeps expensive parsing in the compiled Go tool (srcc) while allowing
Starlark to orchestrate the build graph based on the resolved artifacts.
"""

def _compile_impl(
        template_ctx,
        input_directories,
        output_directories,
        additional_inputs,
        tools,
        **_kwargs):
    """Template implementation that reads manifests and creates compile actions."""
    manifest_dir = input_directories["manifests"]
    output_dir = output_directories["outs"]
    srcc = tools["srcc"]
    srcs = additional_inputs["srcs"].to_list()

    # Build lookup: manifest basename (without .deps) -> manifest file
    manifests_by_name = {}
    for m in manifest_dir.children:
        if m.basename.endswith(".deps"):
            name = m.basename[:-5]  # Remove .deps suffix
            manifests_by_name[name] = m

    # First pass: declare all output files and build path -> output lookup
    # Output path mirrors source path structure: src/lib/foo.src -> src/lib/foo.out
    out_files = {}
    out_by_path = {}  # Map output exec path -> declared output file
    for src in srcs:
        out_path = src.path.replace(".src", ".out")
        out_file = template_ctx.declare_file(out_path, directory = output_dir)
        out_files[src.path] = out_file
        out_by_path[out_file.path] = out_file

    # Second pass: create compile actions with dependencies from manifests
    for src in srcs:
        out_file = out_files[src.path]

        # Find the manifest for this source
        src_name = src.basename[:-4]  # Remove .src suffix
        manifest = manifests_by_name.get(src_name)

        # Read the manifest to get dependency paths, then resolve to declared outputs
        # NOTE: read_artifacts() resolves paths to KNOWN INPUT artifacts.
        # For dependencies that are OTHER OUTPUTS in the same map_directory call,
        # we need to look them up in our out_by_path dictionary instead.
        #
        # The read_artifacts() API is most useful when you have:
        # - A manifest file produced by a previous action
        # - That lists paths to files in additional_inputs or tools
        #
        # For this example, the manifest contains paths to OUTPUT files, so we
        # parse it manually and look up the declared outputs.
        deps = []
        if manifest:
            # Read manifest content and resolve each path to a declared output
            # We'll read the manifest directly (since it's an input tree file)
            manifest_deps = template_ctx.read_artifacts(manifest)
            # These are source files that this file imports
            for dep_src in manifest_deps:
                # Look up the corresponding output file
                dep_out = out_files.get(dep_src.path)
                if dep_out:
                    deps.append(dep_out)

        # Build compile command
        args = template_ctx.args()
        args.add("compile")
        args.add("--input", src)
        args.add("--output", out_file)
        for dep in deps:
            args.add("--dep", dep)

        template_ctx.run(
            inputs = [src] + list(deps),
            outputs = [out_file],
            executable = srcc,
            arguments = [args],
            progress_message = "Compiling %s" % src.short_path,
        )

def _mod_library_impl(ctx):
    """Implementation of mod_library rule."""
    srcs = ctx.files.srcs
    srcc = ctx.executable._srcc
    name = ctx.label.name

    # Declare tree artifacts
    manifest_dir = ctx.actions.declare_directory(name + "_manifests")
    output_dir = ctx.actions.declare_directory(name + "_outs")

    # Write source list file for the resolver
    # Format: <logical_path>:<exec_path> per line
    # For source files, these are the same. For generated files, exec_path includes bazel-out/...
    # The logical path is used to match import statements, the exec path is written to manifests.
    srcs_list_file = ctx.actions.declare_file(name + "_srcs.txt")
    lines = []
    for src in srcs:
        # Logical path: strip bazel-out/.../bin/ prefix if present to get the "import" path
        logical_path = src.short_path
        exec_path = src.path
        lines.append("{}:{}".format(logical_path, exec_path))
    ctx.actions.write(
        output = srcs_list_file,
        content = "\n".join(lines),
    )

    # Action 1: Resolver parses imports and writes per-file manifest files
    # Each manifest contains exec paths of SOURCE FILES that this file imports
    ctx.actions.run(
        mnemonic = "ResolveImports",
        executable = srcc,
        arguments = [
            "resolve",
            "--srcs-file",
            srcs_list_file.path,
            "--manifest-dir",
            manifest_dir.path,
        ],
        inputs = srcs + [srcs_list_file],
        outputs = [manifest_dir],
        progress_message = "Resolving imports for %s" % ctx.label,
    )

    # Action 2: map_directory creates compile actions with correct dependencies
    # The key is that read_artifacts() resolves manifest paths to actual File objects
    ctx.actions.map_directory(
        implementation = _compile_impl,
        input_directories = {"manifests": manifest_dir},
        output_directories = {"outs": output_dir},
        additional_inputs = {"srcs": depset(srcs)},
        tools = {"srcc": ctx.attr._srcc.files_to_run},
    )

    return [DefaultInfo(files = depset([output_dir]))]

mod_library = rule(
    implementation = _mod_library_impl,
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".src"],
            doc = "Source files to compile. Import statements determine dependencies.",
        ),
        "_srcc": attr.label(
            default = "//compiler:srcc",
            executable = True,
            cfg = "exec",
            doc = "The compiler tool.",
        ),
    },
    doc = """Compiles .src files with dynamic dependency resolution.

This rule demonstrates the read_artifacts() API. Source files can contain
"import path/to/file.src" statements. The compiler:

1. First resolves all imports and writes manifest files containing source paths
2. Then map_directory uses read_artifacts() to resolve manifest paths to source Files
3. Finally creates compile actions with the correct output dependencies

Example:
    mod_library(
        name = "mylib",
        srcs = glob(["src/**/*.src"]),
    )
""",
)
