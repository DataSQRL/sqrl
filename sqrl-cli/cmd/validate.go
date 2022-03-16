package cmd

import (
  "strings"

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
  Run: func(cmd *cobra.Command, args []string) {
    fileName := args[0]

    payload, hasSchema, err := assembleScriptBundle(fileName, "", true, cmd)
    if err != nil {
      cmd.PrintErrln(err)
    }

    if verbose {
      cmd.Println("Posting script bundle to resource [/deployment/compile]")
    }
    compilation, err := api.Post2API(clientConfig, "/deployment/compile", payload)
    if err != nil {
      cmd.PrintErrln(err)
    } else {
      success := strings.EqualFold("success",compilation["status"].(string))
      compiletime := compilation["compilationTime"].(int)
      if success {
        cmd.Printf("Successful compilation took %d ms\n", compiletime)
      } else {
        cmd.PrintErrf("Failed compilation took %d ms\n", compiletime)
      }
      printCompilationMessages(compilation, cmd)

      if !hasSchema && success {
        err := saveCompiledSchema(compilation, cmd)
        if err != nil {
          cmd.PrintErrln("Could not write pre-schema to file", err)
        }
      }
    }

  },
}
