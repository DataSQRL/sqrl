package cmd

import (
  "fmt"

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
  Run: func(cmd *cobra.Command, args []string) {
    fmt.Println("Query: " + args[0])
  },
}
