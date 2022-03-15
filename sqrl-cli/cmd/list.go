package cmd

import (
  "fmt"

  "github.com/spf13/cobra"

  "github.com/DataSQRL/datasqrl/cli/pkg/api"
)

func init() {
  rootCmd.AddCommand(listCmd)
  listCmd.AddCommand(listDeployCmd)
  listCmd.AddCommand(listSourcesCmd)
  listCmd.AddCommand(listSinksCmd)
}

var listCmd = &cobra.Command{
  Use:   "list",
  Short: "List all scripts, sources, or sinks on the DataSQLR server",
  Long:  `Prints a list of all the scripts, sources, or sinks that are running or
connected to a DataSQRL server instance`,
}

var listDeployCmd = &cobra.Command{
  Use:   "deployments",
  Short: "List all deployments running on the DataSQLR server",
  Long:  `Prints a list of all the deployments that are running on a DataSQRL server instance`,
  Args: cobra.NoArgs,
  Run: func(cmd *cobra.Command, args []string) {
    fmt.Println("List deployments")
  },
}

var listSourcesCmd = &cobra.Command{
  Use:   "sources",
  Short: "List all data sources connected to the DataSQLR server",
  Long:  `Prints a list of all the data sources that are connected to a DataSQRL server instance`,
  Args: cobra.NoArgs,
  Run: func(cmd *cobra.Command, args []string) {
    results, err := api.GetMultipleFromAPI(serverConfig, "/source")
    if err != nil {
      cmd.PrintErrln(err)
    }
    for idx, result := range results {
      cmd.Printf("Source #%d:", idx)
      cmd.Println(result)
    }
  },
}

var listSinksCmd = &cobra.Command{
  Use:   "sinks",
  Short: "List all data sinks connected to the DataSQLR server",
  Long:  `Prints a list of all the data sinks that are connected to a DataSQRL server instance`,
  Args: cobra.NoArgs,
  Run: func(cmd *cobra.Command, args []string) {
    fmt.Println("List sinks")
  },
}
