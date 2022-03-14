package cmd

import (
  "fmt"

  "github.com/spf13/cobra"
)

func init() {
  rootCmd.AddCommand(watchCmd)
}

var watchCmd = &cobra.Command{
  Use:   "watch [script]",
  Short: "Watch SQRL script for development",
  Long:  `Watches provided SQRL script and continuously submits changes to DataSQRL
server for execution. Creates SQRL script if it does not exist.`,
  Args: cobra.ExactArgs(1),
  Example: "datasqrl watch example.sqrl",
  Run: func(cmd *cobra.Command, args []string) {
    fmt.Println("Watch script")
  },
}
