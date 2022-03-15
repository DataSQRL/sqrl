package cmd

import (
  "github.com/spf13/cobra"
)

func init() {
  rootCmd.AddCommand(validateCmd)
}

var validateCmd = &cobra.Command{
  Use:   "validate [script]",
  Short: "Validate SQRL script on DataSQRL server",
  Long:  `Validate the SQRL script (plus associated schema and query templates)
on the DataSQRL server`,
  Args: cobra.ExactArgs(1),
  Example: "datasqrl validate example.sqrl",
  Run: func(cmd *cobra.Command, args []string) {
    cmd.Println("Deploy script")
  },
}
