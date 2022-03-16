package cmd

import (
  "github.com/spf13/cobra"
)

func init() {
  rootCmd.AddCommand(runCmd)
  runCmd.AddCommand(runDevCmd)
  runCmd.AddCommand(runProdCmd)
}

var runCmd = &cobra.Command{
  Use:   "run (dev|prod)",
  Short: "Run DataSQRL server",
  Long:  `Starts a local instance of DataSQRL server for development or production`,
}

var runDevCmd = &cobra.Command{
  Use:   "dev",
  Short: "Run DataSQRL server in development mode",
  Long:  `Starts a local instance of DataSQRL server for development`,
  Run: func(cmd *cobra.Command, args []string) {
    cmd.Println("Run DataSQRL Dev server - to be implemented")
  },
}

var runProdCmd = &cobra.Command{
  Use:   "prod",
  Short: "Run DataSQRL server in production mode",
  Long:  `Starts a local instance of DataSQRL server for production`,
  Run: func(cmd *cobra.Command, args []string) {
    cmd.Println("Run DataSQRL Prod server - to be implemented")
  },
}
