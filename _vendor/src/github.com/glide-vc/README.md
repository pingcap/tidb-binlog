# Glide vendor cleaner

[![Build Status](https://travis-ci.org/sgotti/glide-vc.svg?branch=master)](https://travis-ci.org/sgotti/glide-vc)
[![Build status](https://ci.appveyor.com/api/projects/status/8uukgs49kv9pdee7/branch/master?svg=true)](https://ci.appveyor.com/project/sgotti/glide-vc/branch/master)

## Important Note!!!

Before using this tool be sure that cleaning and commiting vendored directories to VCS does not violate the licenses of the packages you're vendoring.

For a detailed explanation on why Glide doesn't do this see [here](http://engineeredweb.com/blog/2016/go-why-not-strip-unused-pkgs/)

## Description

This tool will help you removing from the project vendor directories all the files not needed for building your project. By default it'll keep only needed packages (returned by the `glide list` command) and all the files inside them.
If you want to keep only source code (including tests) files you can provide the `--only-code` option.
If you want to remove also the go test files you can add the `--no-tests` option.

By default `glide-vc` doesn't remove:

* files that are likely to contain some type of of legal declaration or licensing information (to remove them use the `--no-legal-files` option)
* nested vendor directories. Doing this will change compilation and runtime behavior of your project because only the top level vendored dependencies will be used for compilation. If these are at a different revision (from the one provided inside nested vendor directories) they can cause compilation problems or runtime misbehiaviours. On the other side, keeping nested vendor directories can cause compilation problems like [this one](https://github.com/mattfarina/golang-broken-vendor).

## Vendoring additional tools

In order to vendor tools, some projects will specify packages in their `glide.yaml` that aren't imported by the project. These packages will be installed and registered in the glide.lock file (but this behavior may change in the future).

By default `glide-vc` uses the output of the `glide list` command to retrieve the packages needed by your project. Since `glide list` uses your projects imports it will not include the tools contained in `glide.lock`.

Using the `--use-lock-file` option will make `glide-vc` use the packages list from `glide.lock` instead of the one provided by `glide list`, preserving the tool packages.

Instead of vendoring these tools using glide and using the `glide-vc` `--use-lock-file` option, a suggestion (since there isn't a common accepted practice) is to vendor additional project tools using other scripts/tools and perhaps not inside the `vendor` directory but in another project's path and use the `vendor` directory just for go dependencies (or if you want to keep them inside `vendor` then run your tool after `glide-vc`). See also [this discussion](https://github.com/sgotti/glide-vc/pull/21#issuecomment-246099311).

## Install

`go get github.com/sgotti/glide-vc`

## Run
```
glide vendor cleaner

Usage:
  glide-vc [flags]

Flags:
      --dryrun            just output what will be removed
      --keep value        A pattern to keep additional files inside needed packages. The pattern match will be relative to the deeper vendor dir. Supports double star (**) patterns. (see https://golang.org/pkg/path/filepath/#Match and https://github.com/bmatcu
k/doublestar). Can be specified multiple times. For example to keep all the files with json extension use the '**/*.json' pattern. (default [])
      --no-legal-files    remove also licenses and legal files
      --no-test-imports   remove also testImport vendor directories. Works only with --use-lock-file
      --no-tests          remove also go test files (requires --only-code)
      --only-code         keep only source code files (including go test files)
      --use-lock-file     use glide.lock instead of glide list to determine imports
```

You have to run `glide-vc`, or (if glide is installed) `glide vc` inside your current project root directory.

To see what it'll do use the `--dryrun` option.

### Examples

Tests removal of all unneeded packages.

```
glide-vc --dryrun
```

Do it

```
glide-vc
```


Keep only source code (including tests) files.

```
glide-vc --only-code
```

Keep only source code without go tests.

```
glide-vc --only-code --no-tests
```

Keep only source code without go tests and also remove all licenses and legal files.

```
glide-vc --only-code --no-tests --no-legal-files
```
