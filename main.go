package main

import (
	"github.com/spf13/cobra"
)

func main() {
	cobra.CheckErr(command.Execute())
}