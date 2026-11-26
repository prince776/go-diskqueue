//go:build dstbugs

package diskqueue

import "os"

var activeBug = os.Getenv("DSTBUG")

// bug reports whether the known, fixed bug name is re-inserted: the one
// named by $DSTBUG (only with the dstbugs tag).
func bug(name string) bool { return name != "" && name == activeBug }
