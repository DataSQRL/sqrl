package cmd

import (
  "github.com/spf13/cobra"

  "github.com/DataSQRL/datasqrl/cli/pkg/api"
)

func init() {
  rootCmd.AddCommand(validateCmd)
}

var validateCmd = &cobra.Command{
  Use:   "compile [script]",
  Short: "Compile SQRL script on DataSQRL server",
  Long:  `Compile and validate the SQRL script (plus associated schema and query templates)
on the DataSQRL server`,
  Args: cobra.ExactArgs(1),
  Example: "datasqrl validate example.sqrl",
  RunE: func(cmd *cobra.Command, args []string) error {
    fileName := args[0]

    payload, hasSchema, err := assembleScriptBundle(fileName, "", true, cmd)
    if err != nil {
      return err
    }

    if verbose {
      cmd.Println("Posting script bundle to resource [/compile]")
    }
    compilationResult, err := api.Post2API(clientConfig, "/compile", payload)
    if err != nil {
      return err
    } else {
      printCompilationResult(compilationResult, cmd)
      if !hasSchema {
        err := saveCompiledSchemas(compilationResult, cmd)
        if err != nil {
          return err
        }
      }
      return nil
    }

  },
}
