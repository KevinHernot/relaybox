// Package relaybox provides small, production-minded building blocks for
// reliable event delivery in Go services.
package relaybox

const Version = "0.1.0-dev"

// Project describes the current package metadata.
type Project struct {
	Name        string
	Version     string
	Description string
}

// Current returns the current package metadata.
func Current() Project {
	return Project{
		Name:        "relaybox",
		Version:     Version,
		Description: "Reliable event delivery primitives for Go services.",
	}
}
