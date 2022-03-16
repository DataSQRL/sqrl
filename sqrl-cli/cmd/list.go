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

var deployment2StringFct = get2StringFctByKeys([]string{"name","version","id","submissionTime","status"})

var listDeployCmd = &cobra.Command{
  Use:   "deployments",
  Short: "List all deployments running on the DataSQLR server",
  Long:  `Prints a list of all the deployments that are running on a DataSQRL server instance`,
  Args: cobra.NoArgs,
  Run: func(cmd *cobra.Command, args []string) {
    if verbose {
      cmd.Println("Getting deployments from resource [/deployment]")
    }
    results, err := api.GetMultipleFromAPI(clientConfig, "/deployment")
    if err != nil {
      cmd.PrintErrln(err)
    }
    displayResults(results, args, cmd, getNameFunction("name"), deployment2StringFct)
  },
}

var listSourcesCmd = &cobra.Command{
  Use:   "sources [names]",
  Short: "List all data sources connected to the DataSQLR server",
  Long:  `Prints a list of all the data sources that are connected to a DataSQRL server instance.
If additional argument names are provided, only sources with those names are shown.`,
  Args: cobra.ArbitraryArgs,
  Run: func(cmd *cobra.Command, args []string) {
    if verbose {
      cmd.Println("Getting sources from resource [/source]")
    }
    results, err := api.GetMultipleFromAPI(clientConfig, "/source")
    if err != nil {
      cmd.PrintErrln(err)
    }
    displayResults(results, args, cmd, getNameFunction("sourceName"), payload2StringTopLevel)
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


func displayResults(results []api.Payload, nameFilter []string, cmd *cobra.Command,
                    nameFct func(payload api.Payload) string, toStringFct func(result api.Payload) string) {
  filterSources := len(nameFilter)>0
  if verbose && filterSources {
    cmd.Println("Only showing sources with names in: ", nameFilter)
  }
  for _, result := range results {
    if !filterSources || containsIgnoreCase(nameFilter, nameFct(result)) {
      cmd.Println(toStringFct(result))
      cmd.Println("---")
    }
  }
}

func getNameFunction(nameKey string) func(payload api.Payload) string {
  return func(payload api.Payload) string {
    return payload[nameKey].(string)
  }
}
