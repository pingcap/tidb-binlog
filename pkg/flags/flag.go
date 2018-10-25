package flags

import (
	"flag"
	"net/url"
	"os"
	"strings"

	"github.com/juju/errors"
)

func flagToEnv(prefix, name string) string {
	return prefix + "_" + strings.ToUpper(strings.Replace(name, "-", "_", -1))
}

// SetFlagsFromEnv parses all registered flags in the given flagset,
// and if they are not already set it attempts to set their values from
// environment variables. Environment variables take the name of the flag but
// are UPPERCASE, have the given prefix and any dashes are replaced by
// underscores - for example: some-flag => PUMP_SOME_FLAG
func SetFlagsFromEnv(prefix string, fs *flag.FlagSet) error {
	var err error
	alreadySet := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		alreadySet[flagToEnv(prefix, f.Name)] = true
	})
	usedEnvKey := make(map[string]bool)
	fs.VisitAll(func(f *flag.Flag) {
		err = setFlagFromEnv(fs, prefix, f.Name, usedEnvKey, alreadySet)
	})

	return errors.Trace(err)
}

type flagSetter interface {
	Set(fk string, fv string) error
}

func setFlagFromEnv(fs flagSetter, prefix, fname string, usedEnvKey, alreadySet map[string]bool) error {
	key := flagToEnv(prefix, fname)
	if !alreadySet[key] {
		val := os.Getenv(key)
		if val != "" {
			usedEnvKey[key] = true
			if serr := fs.Set(fname, val); serr != nil {
				return errors.Errorf("invalid environment value %q for %s: %v", val, key, serr)
			}
			// recognized and used environment variable key=val
		}
	}
	return nil
}

// URLsFromFlag returns a slices from url got from the flag.
func URLsFromFlag(fs *flag.FlagSet, urlsFlagName string) []url.URL {
	return fs.Lookup(urlsFlagName).Value.(*URLsValue).URLSlice()
}

// URLStrsFromFlag returns a string slices from url got from the flag.
func URLStrsFromFlag(fs *flag.FlagSet, urlsFlagName string) []string {
	return fs.Lookup(urlsFlagName).Value.(*URLsValue).StringSlice()
}
