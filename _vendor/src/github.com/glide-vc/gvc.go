package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/Masterminds/glide/cfg"
	gpath "github.com/Masterminds/glide/path"
	"github.com/bmatcuk/doublestar"
	"github.com/spf13/cobra"
)

var cmd = &cobra.Command{
	Use:   "glide-vc",
	Short: "glide vendor cleaner",
	Run:   glidevc,
}

type options struct {
	dryrun       bool
	onlyCode     bool
	noTests      bool
	noLegalFiles bool
	keepPatterns []string

	// Deprecated
	useLockFile   bool
	noTestImports bool
}

type packages struct {
	Installed []string `json:"installed"`
	Missing   []string `json:"missing"`
	Gopath    []string `json:"gopath"`
}

var (
	opts         options
	codeSuffixes = []string{".go", ".c", ".s", ".S", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx"}
)

const (
	goTestSuffix = "_test.go"
)

func init() {
	cmd.PersistentFlags().BoolVar(&opts.dryrun, "dryrun", false, "just output what will be removed")
	cmd.PersistentFlags().BoolVar(&opts.onlyCode, "only-code", false, "keep only source code files (including go test files)")
	cmd.PersistentFlags().BoolVar(&opts.noTests, "no-tests", false, "remove also go test files (requires --only-code)")
	cmd.PersistentFlags().BoolVar(&opts.noLegalFiles, "no-legal-files", false, "remove also licenses and legal files")
	cmd.PersistentFlags().StringSliceVar(&opts.keepPatterns, "keep", []string{}, "A pattern to keep additional files inside needed packages. The pattern match will be relative to the deeper vendor dir. Supports double star (**) patterns. (see https://golang.org/pkg/path/filepath/#Match and https://github.com/bmatcuk/doublestar). Can be specified multiple times. For example to keep all the files with json extension use the '**/*.json' pattern.")

	cmd.PersistentFlags().BoolVar(&opts.useLockFile, "use-lock-file", false, "use glide.lock instead of glide list to determine imports")
	cmd.PersistentFlags().BoolVar(&opts.noTestImports, "no-test-imports", false, "remove also testImport vendor directories. Works only with --use-lock-file")
}

func main() {
	cmd.Execute()
}

func glidevc(cmd *cobra.Command, args []string) {
	if opts.noTests && !opts.onlyCode {
		fmt.Fprintln(os.Stderr, "--no-tests requires --only-code")
		os.Exit(1)
	}

	if err := cleanup("."); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func glideLockImports(path string) ([]string, error) {
	yml, err := ioutil.ReadFile(filepath.Join(path, gpath.LockFile))
	if err != nil {
		return nil, err
	}

	lock, err := cfg.LockfileFromYaml(yml)
	if err != nil {
		return nil, err
	}

	var imports []string
	adder := func(locks cfg.Locks) {
		for _, lock := range locks {
			for _, subpackage := range lock.Subpackages {
				imports = append(imports, lock.Name+"/"+subpackage)
			}
			imports = append(imports, lock.Name)
		}
	}

	adder(lock.Imports)
	if !opts.noTestImports {
		adder(lock.DevImports)
	}

	return imports, nil
}

func glideListImports(path string) ([]string, error) {
	cmd := exec.Command("glide", "list", "-output", "json", path)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	list := &packages{}
	err = json.Unmarshal(out, list)
	if err != nil {
		return nil, err
	}

	return list.Installed, nil
}

func cleanup(path string) error {
	var (
		packages []string
		err      error
	)

	if opts.useLockFile {
		packages, err = glideLockImports(path)
	} else {
		packages, err = glideListImports(path)
	}
	if err != nil {
		return err
	}

	// The package list already have the path converted to the os specific
	// path separator, needed for future comparisons.
	pkgList := []string{}
	pkgMap := map[string]struct{}{}
	for _, imp := range packages {
		if _, found := pkgMap[imp]; !found {
			// This converts pkg separator "/" to os specific separator
			pkgList = append(pkgList, filepath.FromSlash(imp))
			pkgMap[imp] = struct{}{}
		}
	}

	vpath, err := gpath.Vendor()
	if err != nil {
		return err
	}
	if vpath == "" {
		return fmt.Errorf("cannot find vendor dir")
	}

	type pathData struct {
		path  string
		isDir bool
	}
	var searchPath string
	markForKeep := map[string]pathData{}
	markForDelete := []pathData{}

	// Walk vendor directory
	searchPath = vpath + string(os.PathSeparator)
	err = filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if path == searchPath || path == vpath {
			return nil
		}

		// Short-circuit for test files
		if opts.noTests && strings.HasSuffix(path, "_test.go") {
			return nil
		}

		localPath := strings.TrimPrefix(path, searchPath)

		lastVendorPath, err := getLastVendorPath(localPath)
		if err != nil {
			return err
		}
		lastVendorPathDir := filepath.Dir(lastVendorPath)

		keep := false

		for _, name := range pkgList {
			// if a directory is a needed package then keep it
			keep = keep || info.IsDir() && name == lastVendorPath

			// The remaining tests are only for files
			if info.IsDir() {
				continue
			}

			// Keep legal files in directories that are the parent of a needed package
			keep = keep || !opts.noLegalFiles && IsLegalFile(path) && isParentDirectory(lastVendorPathDir, name)

			// The remaining tests only apply if the file is in a needed package
			if name != lastVendorPathDir {
				continue
			}

			// Keep everything unless --only-code was specified
			keep = keep || !opts.onlyCode

			// Always keep code files
			for _, suffix := range codeSuffixes {
				keep = keep || strings.HasSuffix(path, suffix)
			}

			// Match keep patterns
			for _, keepPattern := range opts.keepPatterns {
				ok, err := doublestar.Match(keepPattern, lastVendorPath)
				// TODO(sgotti) if a bad pattern is encountered stop here. Actually there's no function to verify a pattern before using it, perhaps just a fake match at the start will work.
				if err != nil {
					return fmt.Errorf("bad pattern: %q", keepPattern)
				}
				keep = keep || ok
			}
		}

		if keep {
			// Keep all parent directories of current path
			for curpath := localPath; curpath != "."; curpath = filepath.Dir(curpath) {
				markForKeep[curpath] = pathData{curpath, true}
			}
			// Fix isDir property for current path
			markForKeep[localPath] = pathData{localPath, info.IsDir()}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Generate deletion list
	err = filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Ignore not existant files due to previous removal of the parent directory
			if !os.IsNotExist(err) {
				return err
			}
		}
		localPath := strings.TrimPrefix(path, searchPath)
		if localPath == "" {
			return nil
		}
		if _, ok := markForKeep[localPath]; !ok {
			markForDelete = append(markForDelete, pathData{path, info.IsDir()})
			if info.IsDir() {
				// skip directory contents since it has been marked for removal
				return filepath.SkipDir
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Perform the actual delete.
	for _, marked := range markForDelete {
		localPath := strings.TrimPrefix(marked.path, searchPath)
		if marked.isDir {
			fmt.Printf("Removing unused dir: %s\n", localPath)
		} else {
			fmt.Printf("Removing unused file: %s\n", localPath)
		}
		if !opts.dryrun {
			rerr := os.RemoveAll(marked.path)
			if rerr != nil {
				return rerr
			}
		}
	}

	return nil
}

func getLastVendorPath(path string) (string, error) {
	for curpath := path; curpath != "."; curpath = filepath.Dir(curpath) {
		if filepath.Base(curpath) == "vendor" {
			return filepath.Rel(curpath, path)
		}
	}
	return path, nil
}

func isParentDirectory(parent, child string) bool {
	if !strings.HasSuffix(parent, string(filepath.Separator)) {
		parent += string(filepath.Separator)
	}
	if !strings.HasSuffix(child, string(filepath.Separator)) {
		child += string(filepath.Separator)
	}
	return strings.HasPrefix(child, parent)
}

// File lists and code took from https://github.com/client9/gosupplychain/blob/master/license.go

// LicenseFilePrefix is a list of filename prefixes that indicate it
//  might contain a software license
var LicenseFilePrefix = []string{
	"licence", // UK spelling
	"license", // US spelling
	"copying",
	"unlicense",
	"copyright",
	"copyleft",
}

// LegalFileSubstring are substrings that indicate the file is likely
// to contain some type of legal declaration.  "legal" is often used
// that it might be moved to LicenseFilePrefix
var LegalFileSubstring = []string{
	"legal",
	"notice",
	"disclaimer",
	"patent",
	"third-party",
	"thirdparty",
}

// IsLegalFile returns true if the file is likely to contain some type
// of of legal declaration or licensing information
func IsLegalFile(path string) bool {
	lowerfile := strings.ToLower(filepath.Base(path))
	for _, prefix := range LicenseFilePrefix {
		if strings.HasPrefix(lowerfile, prefix) && !strings.HasSuffix(lowerfile, goTestSuffix) {
			return true
		}
	}
	for _, substring := range LegalFileSubstring {
		if strings.Contains(lowerfile, substring) && !strings.HasSuffix(lowerfile, goTestSuffix) {
			return true
		}
	}
	return false
}
