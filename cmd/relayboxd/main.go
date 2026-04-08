package main

import (
	"fmt"

	"github.com/kevinhernot/relaybox"
)

func main() {
	project := relaybox.Current()
	fmt.Printf("%s %s\n%s\n", project.Name, project.Version, project.Description)
}
