// srcc is a simple "compiler" for .src files that demonstrates the read_artifacts pattern.
//
// It has two modes:
//   - resolve: Parse import statements from source files and write manifest files
//     containing the exec paths of source dependencies
//   - compile: Compile a source file, reading its dependencies
//
// The .src file format is simple:
//   - Lines starting with "import " specify dependencies (path relative to workspace root)
//   - All other lines are "code" that gets copied to the output
//
// Example .src file:
//
//	import src/lib/helper.src
//	import src/lib/utils.src
//	print("hello world")
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: srcc <command> [args...]")
		fmt.Fprintln(os.Stderr, "Commands: resolve, compile")
		os.Exit(1)
	}

	command := os.Args[1]
	os.Args = os.Args[1:] // Shift args for flag parsing

	switch command {
	case "resolve":
		runResolve()
	case "compile":
		runCompile()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		os.Exit(1)
	}
}

// runResolve parses source files for imports and writes manifest files.
// Each manifest contains the exec paths of the SOURCE files that the source imports.
// These paths can then be resolved by read_artifacts() to actual File objects.
func runResolve() {
	srcsFile := flag.String("srcs-file", "", "File containing list of source files (logical_path:exec_path per line)")
	manifestDir := flag.String("manifest-dir", "", "Directory to write manifest files")
	flag.Parse()

	if *srcsFile == "" || *manifestDir == "" {
		fmt.Fprintln(os.Stderr, "resolve: --srcs-file and --manifest-dir are required")
		os.Exit(1)
	}

	// Read source file list and build mappings
	// Format: logical_path:exec_path
	// logical_path is used to match import statements
	// exec_path is what gets written to manifests (for read_artifacts to resolve)
	lines, err := readLines(*srcsFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read srcs file: %v\n", err)
		os.Exit(1)
	}

	type srcInfo struct {
		logicalPath string
		execPath    string
	}
	var srcs []srcInfo
	logicalToExec := make(map[string]string)

	for _, line := range lines {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "Invalid line in srcs file (expected logical:exec): %s\n", line)
			os.Exit(1)
		}
		logical := parts[0]
		exec := parts[1]
		srcs = append(srcs, srcInfo{logical, exec})
		logicalToExec[logical] = exec
	}

	// Create manifest directory
	if err := os.MkdirAll(*manifestDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create manifest dir: %v\n", err)
		os.Exit(1)
	}

	// Process each source file
	for _, src := range srcs {
		imports, err := parseImports(src.execPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse imports from %s: %v\n", src.execPath, err)
			os.Exit(1)
		}

		// Resolve import paths to exec paths
		// Import statements use logical paths, but manifests need exec paths
		var resolvedImports []string
		for _, imp := range imports {
			execPath, ok := logicalToExec[imp]
			if !ok {
				fmt.Fprintf(os.Stderr, "Unknown import '%s' in %s (not in srcs list)\n", imp, src.execPath)
				os.Exit(1)
			}
			resolvedImports = append(resolvedImports, execPath)
		}

		// Write manifest file with exec paths
		// read_artifacts() will resolve these to the actual source File objects
		baseName := filepath.Base(src.execPath)
		manifestName := strings.TrimSuffix(baseName, ".src") + ".deps"
		manifestPath := filepath.Join(*manifestDir, manifestName)

		content := strings.Join(resolvedImports, "\n")
		if len(resolvedImports) > 0 {
			content += "\n"
		}
		if err := os.WriteFile(manifestPath, []byte(content), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write manifest %s: %v\n", manifestPath, err)
			os.Exit(1)
		}

		fmt.Printf("Wrote manifest: %s (%d deps)\n", manifestPath, len(resolvedImports))
	}
}

// runCompile compiles a single source file.
// It reads the source and its dependencies, then produces an output file.
func runCompile() {
	input := flag.String("input", "", "Input source file")
	output := flag.String("output", "", "Output file")
	var deps arrayFlags
	flag.Var(&deps, "dep", "Dependency file (can be specified multiple times)")
	flag.Parse()

	if *input == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "compile: --input and --output are required")
		os.Exit(1)
	}

	// Read the source file
	sourceLines, err := readLines(*input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read input: %v\n", err)
		os.Exit(1)
	}

	// Read all dependency contents
	var depContents []string
	for _, dep := range deps {
		content, err := os.ReadFile(dep)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read dependency %s: %v\n", dep, err)
			os.Exit(1)
		}
		depContents = append(depContents, string(content))
	}

	// Create output directory if needed
	if err := os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output dir: %v\n", err)
		os.Exit(1)
	}

	// Write output file
	f, err := os.Create(*output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Write header
	fmt.Fprintf(f, "# Compiled from: %s\n", *input)
	fmt.Fprintf(f, "# Dependencies: %d\n", len(deps))
	fmt.Fprintln(f, "")

	// Include dependency contents (simulating linking)
	for i, dep := range deps {
		fmt.Fprintf(f, "# --- Begin included: %s ---\n", dep)
		fmt.Fprint(f, depContents[i])
		fmt.Fprintf(f, "# --- End included: %s ---\n\n", dep)
	}

	// Write source code (excluding import lines)
	fmt.Fprintln(f, "# --- Source code ---")
	for _, line := range sourceLines {
		if !strings.HasPrefix(line, "import ") {
			fmt.Fprintln(f, line)
		}
	}

	fmt.Printf("Compiled: %s -> %s\n", *input, *output)
}

// parseImports reads a source file and extracts import paths.
func parseImports(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var imports []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "import ") {
			importPath := strings.TrimSpace(strings.TrimPrefix(line, "import "))
			imports = append(imports, importPath)
		}
	}
	return imports, scanner.Err()
}

// readLines reads a file and returns non-empty lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}

// arrayFlags allows multiple -dep flags
type arrayFlags []string

func (a *arrayFlags) String() string {
	return strings.Join(*a, ", ")
}

func (a *arrayFlags) Set(value string) error {
	*a = append(*a, value)
	return nil
}
