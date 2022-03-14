package cmd

import (
  "fmt"

  "github.com/spf13/cobra"
)

func init() {
  rootCmd.AddCommand(deployCmd)
}

var deployCmd = &cobra.Command{
  Use:   "deploy [script]",
  Short: "Deploy SQRL script for execution on DataSQRL server",
  Long:  `Deploy the SQRL script (plus associated schema and query templates)
to DataSQRL server for execution`,
  Args: cobra.ExactArgs(1),
  Example: "datasqrl deploy example.sqrl",
  Run: func(cmd *cobra.Command, args []string) {
    fmt.Println("Deploy script")
  },
}
