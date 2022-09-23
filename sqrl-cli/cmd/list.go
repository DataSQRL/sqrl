package cmd

import (
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
	Long: `Prints a list of all the scripts, sources, or sinks that are running or
connected to a DataSQRL server instance`,
}

var deployment2StringFct = get2StringFctByKeys([]string{"name", "version", "id", "submissionTime", "status"})

var listDeployCmd = &cobra.Command{
	Use:   "deployments",
	Short: "List all deployments running on the DataSQLR server",
	Long:  `Prints a list of all the deployments that are running on a DataSQRL server instance`,
	Args:  cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if verbose {
			cmd.Println("Getting deployments from resource [/deployment]")
		}
		results, err := api.GetMultipleFromAPI(clientConfig, "/deployment")
		if err != nil {
			return err
		}
		displayResults(results, args, cmd, getNameFunction("name"), deployment2StringFct)
		return nil
	},
}

var listSourcesCmd = &cobra.Command{
	Use:   "sources [names]",
	Short: "List all data sources connected to the DataSQLR server",
	Long: `Prints a list of all the data sources that are connected to a DataSQRL server instance.
If additional argument names are provided, only sources with those names are shown.`,
	Args: cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if verbose {
			cmd.Println("Getting sources from resource [/source]")
		}
		results, err := api.GetMultipleFromAPI(clientConfig, "/source")
		if err != nil {
			return err
		}
		//displayResults(results, args, cmd, getNameFunction("sourceName"), payload2StringTopLevel)
		DisplayError(results)
		return nil
	},
}

var listSinksCmd = &cobra.Command{
	Use:   "sinks",
	Short: "List all data sinks connected to the DataSQLR server",
	Long:  `Prints a list of all the data sinks that are connected to a DataSQRL server instance`,
	Args:  cobra.ArbitraryArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.Println("List sinks")
		return nil
	},
}

func displayResults(results []api.Payload, nameFilter []string, cmd *cobra.Command,
	nameFct func(payload api.Payload) string, toStringFct func(result api.Payload) string) {
	filterSources := len(nameFilter) > 0
	if verbose && filterSources {
		cmd.Println("Only showing sources with names in: ", nameFilter)
	}
	if len(results) == 0 {
		cmd.Println("Received 0 results")
	}
	count := 0
	for _, result := range results {
		if !filterSources || containsIgnoreCase(nameFilter, nameFct(result)) {
			count++
			cmd.Println("---Result", count)
			cmd.Print(toStringFct(result))
			cmd.Println("---")
		}
	}
}

func getNameFunction(nameKey string) func(payload api.Payload) string {
	return func(payload api.Payload) string {
		return payload[nameKey].(string)
	}
}
