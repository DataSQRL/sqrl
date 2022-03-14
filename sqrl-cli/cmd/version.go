package cmd

import (
  "fmt"

  "github.com/spf13/cobra"
)

func init() {
  rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
  Use:   "version",
  Short: "Version number of DataSQRL CLI",
  Long:  `Print the version number of DataSQRL CLI`,
  Args: cobra.NoArgs,
  Run: func(cmd *cobra.Command, args []string) {
    fmt.Println("DataSQRL Command Line Utility v0.1 -- ALPHA")
  },
}
