package cmd

import (
  "fmt"

  "github.com/spf13/cobra"
)

func init() {
  rootCmd.AddCommand(submitCmd)
}

var submitCmd = &cobra.Command{
  Use:   "submit [script]",
  Short: "Submit SQRL script for execution on DataSQRL server",
  Long:  `Submits the SQRL script (plus associated schema and query templates)
to DataSQRL server for execution`,
  Args: cobra.ExactArgs(1),
  Example: "datasqrl submit example.sqrl",
  Run: func(cmd *cobra.Command, args []string) {
    fmt.Println("Submit script")
  },
}
