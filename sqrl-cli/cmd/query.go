package cmd

import (
  "github.com/spf13/cobra"
)

func init() {
  rootCmd.AddCommand(queryCmd)
}

var queryCmd = &cobra.Command{
  Use:   "query [template] [parameters]",
  Short: "Execute query against DataSQRL API",
  Long:  `Execute query from provided query template against DataSQRL API with the given query parameters`,
  Args: cobra.MinimumNArgs(1),
  Example: "datasqrl query product.gql 50",
  RunE: func(cmd *cobra.Command, args []string) error {
    cmd.Printf("Execute query [%s] with arguments: %s",args[0],args[1:len(args)])
    return nil
  },
}
