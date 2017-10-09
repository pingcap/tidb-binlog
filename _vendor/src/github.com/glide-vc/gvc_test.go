package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type FileInfo struct {
	path  string
	isDir bool
}

func createVendorTree(t *testing.T, dir string, tree []FileInfo) error {
	for _, fi := range tree {
		path := filepath.Join(dir, "vendor", fi.path)
		if fi.isDir {
			if err := os.MkdirAll(path, 0777); err != nil {
				return fmt.Errorf("failed to create dir %q: %v", filepath.Dir(path), err)
			}
		} else {
			// Create parent dir
			if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
				return fmt.Errorf("failed to create dir %q: %v", filepath.Dir(path), err)
			}
			f, err := os.Create(path)
			if err != nil {
				return fmt.Errorf("failed to create file %q: %v", path, err)
			}
			if strings.HasSuffix(path, ".go") {
				fmt.Fprintf(f, "package %s", filepath.Base(filepath.Dir(path)))
			}
			f.Close()
		}
	}
	return nil
}

func checkExpectedVendor(t *testing.T, dir string, exp []FileInfo) error {
	vendorPath := filepath.Join(dir, "vendor")

	// Walk all files and check everything is defined in exp
	err := filepath.Walk(vendorPath, func(path string, info os.FileInfo, err error) error {
		if path == vendorPath {
			return nil
		}
		for _, fi := range exp {
			if filepath.Join(dir, "vendor", fi.path) == path {
				if fi.isDir != info.IsDir() {
					return fmt.Errorf("mismatching type for %s, expected dir: %t, got dir: %t", fi.path, fi.isDir, info.IsDir())
				}
				return nil
			}
		}
		return fmt.Errorf("file %s shouldn't exist", path)
	})

	// Check that all files in exp exists in vendor dir
	for _, fi := range exp {
		vfi, err := os.Stat(filepath.Join(vendorPath, fi.path))
		if err != nil {
			return fmt.Errorf("error searching for file %s: %v", fi.path, err)
		}
		if fi.isDir != vfi.IsDir() {
			return fmt.Errorf("mismatching type for %s, expected dir: %t, got dir: %t", fi.path, fi.isDir, vfi.IsDir())
		}
	}
	return err
}

type testData struct {
	tree          []FileInfo
	lockdata      string
	mainfile      string
	expectedFiles []FileInfo
	opts          options
}

func TestCleanup(t *testing.T) {

	tree := []FileInfo{
		// Needed dependency
		{"host01/org01/repo01/README", false},
		{"host01/org01/repo01/LICENSE", false},
		{"host01/org01/repo01/file01.go", false},
		{"host01/org01/repo01/file01_test.go", false},
		{"host01/org01/repo01/subpkg01/LICENSE", false},
		{"host01/org01/repo01/subpkg01/file02.go", false},
		{"host01/org01/repo01/subpkg01/file02_test.go", false},
		{"host01/org01/repo01/subpkg01/file03.c", false},
		{"host01/org01/repo01/subpkg01/file04.s", false},
		{"host01/org01/repo01/subpkg01/file05.S", false},
		{"host01/org01/repo01/subpkg01/file06.cc", false},
		{"host01/org01/repo01/subpkg01/file07.cpp", false},
		{"host01/org01/repo01/subpkg01/file09.cxx", false},
		{"host01/org01/repo01/subpkg01/file10.h", false},
		{"host01/org01/repo01/subpkg01/file11.hh", false},
		{"host01/org01/repo01/subpkg01/file12.hpp", false},
		{"host01/org01/repo01/subpkg01/file13.hxx", false},
		{"host01/org01/repo01/subpkg01/file.json", false},
		// Unneeded project inside nested vendor
		{"host01/org01/repo01/vendor/host03/org03/repo03/LICENSE", false},
		{"host01/org01/repo01/vendor/host03/org03/repo03/file05.go", false},
		{"host01/org01/repo01/vendor/host03/org03/repo03/file05_test.go", false},
		// Needed project inside nested vendor
		{"host01/org01/repo01/vendor/host02/org02/repo02/README", false},
		{"host01/org01/repo01/vendor/host02/org02/repo02/LICENSE", false},
		{"host01/org01/repo01/vendor/host02/org02/repo02/file03.go", false},
		{"host01/org01/repo01/vendor/host02/org02/repo02/file03_test.go", false},
		{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/LICENSE", false},
		{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/file04.go", false},
		{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/file04_test.go", false},
		// Unneeded nested vendor inside needed project in nested vendor
		{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/vendor/host04/org04/repo04/LICENSE", false},
		{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/vendor/host04/org04/repo04/file04.go", false},
		{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/vendor/host04/org04/repo04/file04_test.go", false},
		{"host02/org02/repo02/README", false},
		{"host02/org02/repo02/LICENSE", false},
		{"host02/org02/repo02/file03.go", false},
		{"host02/org02/repo02/file03_test.go", false},
		{"host02/org02/repo02/subpkg02/LICENSE", false},
		{"host02/org02/repo02/subpkg02/file04.go", false},
		{"host02/org02/repo02/subpkg02/file04_test.go", false},
	}

	lockdata := `
hash: 4e9eb8fc04548f539b83a52ce8c2001573802b21c903fca974442e79b4690713
updated: 2016-03-04T15:02:44.735574617+01:00
imports:
- name: host01/org01/repo01
  version: 76626ae9c91c4f2a10f34cad8ce83ea42c93bb75
  subpackages:
  - subpkg01
- name: host02/org02/repo02
  version: 76626ae9c91c4f2a10f34cad8ce83ea42c93bb75
  subpackages:
  - subpkg02
devImports: []
`

	mainfile := `package main

import (
	_ "host01/org01/repo01"
	_ "host01/org01/repo01/subpkg01"
	_ "host02/org02/repo02"
	_ "host02/org02/repo02/subpkg02"
)
`

	tests := []testData{
		{
			tree:     tree,
			lockdata: lockdata,
			mainfile: mainfile,
			expectedFiles: []FileInfo{
				{"host01", true},
				{"host01/org01", true},
				{"host01/org01/repo01", true},
				{"host01/org01/repo01/file01.go", false},
				{"host01/org01/repo01/subpkg01", true},
				{"host01/org01/repo01/subpkg01/file02.go", false},
				{"host01/org01/repo01/subpkg01/file03.c", false},
				{"host01/org01/repo01/subpkg01/file04.s", false},
				{"host01/org01/repo01/subpkg01/file05.S", false},
				{"host01/org01/repo01/subpkg01/file06.cc", false},
				{"host01/org01/repo01/subpkg01/file07.cpp", false},
				{"host01/org01/repo01/subpkg01/file09.cxx", false},
				{"host01/org01/repo01/subpkg01/file10.h", false},
				{"host01/org01/repo01/subpkg01/file11.hh", false},
				{"host01/org01/repo01/subpkg01/file12.hpp", false},
				{"host01/org01/repo01/subpkg01/file13.hxx", false},
				{"host01/org01/repo01/subpkg01/file.json", false},
				{"host01/org01/repo01/vendor", true},
				{"host01/org01/repo01/vendor/host02", true},
				{"host01/org01/repo01/vendor/host02/org02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02/file03.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/file04.go", false},
				{"host02", true},
				{"host02/org02", true},
				{"host02/org02/repo02", true},
				{"host02/org02/repo02/file03.go", false},
				{"host02/org02/repo02/subpkg02", true},
				{"host02/org02/repo02/subpkg02/file04.go", false},
			},
			opts: options{onlyCode: true, noTests: true, noLegalFiles: true, keepPatterns: []string{"**/*.json"}},
		},

		{
			tree:     tree,
			lockdata: lockdata,
			mainfile: mainfile,
			expectedFiles: []FileInfo{
				{"host01", true},
				{"host01/org01", true},
				{"host01/org01/repo01", true},
				{"host01/org01/repo01/LICENSE", false},
				{"host01/org01/repo01/file01.go", false},
				{"host01/org01/repo01/subpkg01", true},
				{"host01/org01/repo01/subpkg01/LICENSE", false},
				{"host01/org01/repo01/subpkg01/file02.go", false},
				{"host01/org01/repo01/subpkg01/file03.c", false},
				{"host01/org01/repo01/subpkg01/file04.s", false},
				{"host01/org01/repo01/subpkg01/file05.S", false},
				{"host01/org01/repo01/subpkg01/file06.cc", false},
				{"host01/org01/repo01/subpkg01/file07.cpp", false},
				{"host01/org01/repo01/subpkg01/file09.cxx", false},
				{"host01/org01/repo01/subpkg01/file10.h", false},
				{"host01/org01/repo01/subpkg01/file11.hh", false},
				{"host01/org01/repo01/subpkg01/file12.hpp", false},
				{"host01/org01/repo01/subpkg01/file13.hxx", false},
				{"host01/org01/repo01/vendor", true},
				{"host01/org01/repo01/vendor/host02", true},
				{"host01/org01/repo01/vendor/host02/org02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02/LICENSE", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/file03.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/LICENSE", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/file04.go", false},
				{"host02", true},
				{"host02/org02", true},
				{"host02/org02/repo02", true},
				{"host02/org02/repo02/LICENSE", false},
				{"host02/org02/repo02/file03.go", false},
				{"host02/org02/repo02/subpkg02", true},
				{"host02/org02/repo02/subpkg02/LICENSE", false},
				{"host02/org02/repo02/subpkg02/file04.go", false},
			},
			opts: options{onlyCode: true, noTests: true},
		},
		{
			tree:     tree,
			lockdata: lockdata,
			mainfile: mainfile,
			expectedFiles: []FileInfo{
				{"host01", true},
				{"host01/org01", true},
				{"host01/org01/repo01", true},
				{"host01/org01/repo01/file01.go", false},
				{"host01/org01/repo01/file01_test.go", false},
				{"host01/org01/repo01/subpkg01", true},
				{"host01/org01/repo01/subpkg01/file02.go", false},
				{"host01/org01/repo01/subpkg01/file02_test.go", false},
				{"host01/org01/repo01/subpkg01/file03.c", false},
				{"host01/org01/repo01/subpkg01/file04.s", false},
				{"host01/org01/repo01/subpkg01/file05.S", false},
				{"host01/org01/repo01/subpkg01/file06.cc", false},
				{"host01/org01/repo01/subpkg01/file07.cpp", false},
				{"host01/org01/repo01/subpkg01/file09.cxx", false},
				{"host01/org01/repo01/subpkg01/file10.h", false},
				{"host01/org01/repo01/subpkg01/file11.hh", false},
				{"host01/org01/repo01/subpkg01/file12.hpp", false},
				{"host01/org01/repo01/subpkg01/file13.hxx", false},
				{"host01/org01/repo01/vendor", true},
				{"host01/org01/repo01/vendor/host02", true},
				{"host01/org01/repo01/vendor/host02/org02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02/file03.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/file03_test.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/file04.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/file04_test.go", false},
				{"host02", true},
				{"host02/org02", true},
				{"host02/org02/repo02", true},
				{"host02/org02/repo02/file03.go", false},
				{"host02/org02/repo02/file03_test.go", false},
				{"host02/org02/repo02/subpkg02", true},
				{"host02/org02/repo02/subpkg02/file04.go", false},
				{"host02/org02/repo02/subpkg02/file04_test.go", false},
			},
			opts: options{onlyCode: true, noLegalFiles: true},
		},
		{
			tree:     tree,
			lockdata: lockdata,
			mainfile: mainfile,
			expectedFiles: []FileInfo{
				{"host01", true},
				{"host01/org01", true},
				{"host01/org01/repo01", true},
				{"host01/org01/repo01/LICENSE", false},
				{"host01/org01/repo01/file01.go", false},
				{"host01/org01/repo01/file01_test.go", false},
				{"host01/org01/repo01/subpkg01", true},
				{"host01/org01/repo01/subpkg01/LICENSE", false},
				{"host01/org01/repo01/subpkg01/file02.go", false},
				{"host01/org01/repo01/subpkg01/file02_test.go", false},
				{"host01/org01/repo01/subpkg01/file03.c", false},
				{"host01/org01/repo01/subpkg01/file04.s", false},
				{"host01/org01/repo01/subpkg01/file05.S", false},
				{"host01/org01/repo01/subpkg01/file06.cc", false},
				{"host01/org01/repo01/subpkg01/file07.cpp", false},
				{"host01/org01/repo01/subpkg01/file09.cxx", false},
				{"host01/org01/repo01/subpkg01/file10.h", false},
				{"host01/org01/repo01/subpkg01/file11.hh", false},
				{"host01/org01/repo01/subpkg01/file12.hpp", false},
				{"host01/org01/repo01/subpkg01/file13.hxx", false},
				{"host01/org01/repo01/vendor", true},
				{"host01/org01/repo01/vendor/host02", true},
				{"host01/org01/repo01/vendor/host02/org02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02/LICENSE", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/file03.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/file03_test.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/LICENSE", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/file04.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/file04_test.go", false},
				{"host02", true},
				{"host02/org02", true},
				{"host02/org02/repo02", true},
				{"host02/org02/repo02/LICENSE", false},
				{"host02/org02/repo02/file03.go", false},
				{"host02/org02/repo02/file03_test.go", false},
				{"host02/org02/repo02/subpkg02", true},
				{"host02/org02/repo02/subpkg02/LICENSE", false},
				{"host02/org02/repo02/subpkg02/file04.go", false},
				{"host02/org02/repo02/subpkg02/file04_test.go", false},
			},
			opts: options{onlyCode: true},
		},
		{
			tree:     tree,
			lockdata: lockdata,
			mainfile: mainfile,
			expectedFiles: []FileInfo{
				{"host01", true},
				{"host01/org01", true},
				{"host01/org01/repo01", true},
				{"host01/org01/repo01/README", false},
				{"host01/org01/repo01/LICENSE", false},
				{"host01/org01/repo01/file01.go", false},
				{"host01/org01/repo01/file01_test.go", false},
				{"host01/org01/repo01/subpkg01", true},
				{"host01/org01/repo01/subpkg01/LICENSE", false},
				{"host01/org01/repo01/subpkg01/file02.go", false},
				{"host01/org01/repo01/subpkg01/file02_test.go", false},
				{"host01/org01/repo01/subpkg01/file03.c", false},
				{"host01/org01/repo01/subpkg01/file04.s", false},
				{"host01/org01/repo01/subpkg01/file05.S", false},
				{"host01/org01/repo01/subpkg01/file06.cc", false},
				{"host01/org01/repo01/subpkg01/file07.cpp", false},
				{"host01/org01/repo01/subpkg01/file09.cxx", false},
				{"host01/org01/repo01/subpkg01/file10.h", false},
				{"host01/org01/repo01/subpkg01/file11.hh", false},
				{"host01/org01/repo01/subpkg01/file12.hpp", false},
				{"host01/org01/repo01/subpkg01/file13.hxx", false},
				{"host01/org01/repo01/subpkg01/file.json", false},
				{"host01/org01/repo01/vendor", true},
				{"host01/org01/repo01/vendor/host02", true},
				{"host01/org01/repo01/vendor/host02/org02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02/README", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/LICENSE", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/file03.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/file03_test.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02", true},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/LICENSE", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/file04.go", false},
				{"host01/org01/repo01/vendor/host02/org02/repo02/subpkg02/file04_test.go", false},
				{"host02", true},
				{"host02/org02", true},
				{"host02/org02/repo02", true},
				{"host02/org02/repo02/README", false},
				{"host02/org02/repo02/LICENSE", false},
				{"host02/org02/repo02/file03.go", false},
				{"host02/org02/repo02/file03_test.go", false},
				{"host02/org02/repo02/subpkg02", true},
				{"host02/org02/repo02/subpkg02/LICENSE", false},
				{"host02/org02/repo02/subpkg02/file04.go", false},
				{"host02/org02/repo02/subpkg02/file04_test.go", false},
			},
		},
	}

	for _, useLockFile := range []bool{false, true} {
		for i, td := range tests {
			t.Logf("Test #%d", i)
			td.opts.useLockFile = useLockFile
			if err := testCleanup(t, &td); err != nil {
				t.Fatalf("#%d: unexpected error: %v", i, err)
			}
		}
	}
}

func testCleanup(t *testing.T, td *testData) error {
	tmpDir, err := ioutil.TempDir("", "glidevc")
	if err != nil {
		return err
	}
	//defer os.RemoveAll(tmpDir)

	wd, _ := os.Getwd()
	defer os.Chdir(wd)
	if err := os.Chdir(tmpDir); err != nil {
		return fmt.Errorf("Could not change to dir %s: %v", wd, err)
	}

	// Create empty glide.yaml (currently not used for hash checking)
	if err := ioutil.WriteFile(filepath.Join(tmpDir, "glide.yaml"), nil, 0666); err != nil {
		return fmt.Errorf("failed to create glide.yaml file: %v", err)
	}

	// Create glide.lock file
	if err := ioutil.WriteFile(filepath.Join(tmpDir, "glide.lock"), []byte(td.lockdata), 0666); err != nil {
		return fmt.Errorf("failed to create glide.lock file: %v", err)
	}

	// Create main.go file
	if err := ioutil.WriteFile(filepath.Join(tmpDir, "main.go"), []byte(td.mainfile), 0666); err != nil {
		return fmt.Errorf("failed to create main.go file: %v", err)
	}

	if err := createVendorTree(t, tmpDir, td.tree); err != nil {
		return err
	}

	opts = td.opts
	if err := cleanup(tmpDir); err != nil {
		return err
	}

	if err := checkExpectedVendor(t, tmpDir, td.expectedFiles); err != nil {
		return err
	}
	return nil
}

func TestGetLastVendorPath(t *testing.T) {
	tests := map[string]string{
		"host1/org1/repo1":                                                 "host1/org1/repo1",
		"host1/org1/repo1/vendor/host2/org2/repo2":                         "host2/org2/repo2",
		"host1/org1/repo1/vendor/host2/org2/repo2/vendor/host3/org3/repo3": "host3/org3/repo3",
	}

	for input, expected := range tests {
		got, err := getLastVendorPath(filepath.FromSlash(input))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if got != filepath.FromSlash(expected) {
			t.Fatalf("got=%q, expected=%q", got, expected)
		}
	}
}

func TestIsParentDirectory(t *testing.T) {
	type testData2 struct {
		Parent string
		Child  string
	}
	tests := map[testData2]bool{
		{"foo", "foo"}:     true,
		{"foo", "foo/bar"}: true,
		{"foo", "foobar"}:  false,
		{"foo/", "foo"}:    true,
		{"foo", "foo/"}:    true,
		{"foo/", "foo/"}:   true,
	}

	for input, expected := range tests {
		got := isParentDirectory(filepath.FromSlash(input.Parent), filepath.FromSlash(input.Child))
		if got != expected {
			t.Fatalf("got=%t, expected=%t", got, expected)
		}
	}
}
