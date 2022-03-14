package cmd

import (
  "fmt"

  "github.com/spf13/cobra"
)

func init() {
  rootCmd.AddCommand(listCmd)
  listCmd.AddCommand(listScriptsCmd)
  listCmd.AddCommand(listSourcesCmd)
  listCmd.AddCommand(listSinksCmd)
}

var listCmd = &cobra.Command{
  Use:   "list (scripts|sources|sinks)",
  Short: "List all scripts, sources, or sinks on the DataSQLR server",
  Long:  `Prints a list of all the scripts, sources, or sinks that are running or
connected to a DataSQRL server instance`,
}

var listScriptsCmd = &cobra.Command{
  Use:   "scripts",
  Short: "List all scripts running on the DataSQLR server",
  Long:  `Prints a list of all the scripts that are running on a DataSQRL server instance`,
  Args: cobra.NoArgs,
  Run: func(cmd *cobra.Command, args []string) {
    fmt.Println("List scripts")
  },
}

var listSourcesCmd = &cobra.Command{
  Use:   "sources",
  Short: "List all data sources connected to the DataSQLR server",
  Long:  `Prints a list of all the data sources that are connected to a DataSQRL server instance`,
  Args: cobra.NoArgs,
  Run: func(cmd *cobra.Command, args []string) {
    fmt.Println("List sources")
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
