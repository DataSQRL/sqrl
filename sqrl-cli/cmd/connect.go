package cmd

import (
  "os"
  // "fmt"
  "path/filepath"
  "github.com/spf13/cobra"
  "github.com/spf13/viper"

  "github.com/DataSQRL/datasqrl/cli/pkg/api"
)

func init() {
  rootCmd.AddCommand(connectCmd)
  connectCmd.AddCommand(connectSourceCmd)
  connectCmd.AddCommand(connectSinkCmd)

  connectSourceCmd.AddCommand(connectSourceFolderCmd)
  connectSourceCmd.AddCommand(connectSourceKafkaCmd)

  connectSinkCmd.AddCommand(connectSinkFolderCmd)
  connectSinkCmd.AddCommand(connectSinkKafkaCmd)
}

var connectCmd = &cobra.Command{
  Use:   "connect",
  Short: "Connect a source or sink to DataSQRL server",
  Long:  `Connect a source or sink to DataSQRL server`,
}

var connectSourceCmd = &cobra.Command{
  Use:   "source",
  Short: "Connect a source to DataSQRL server",
  Long:  `Connect a source to DataSQRL server`,
}

var connectSourceFolderCmd = &cobra.Command{
  Use:   "folder [uri]",
  Short: "Connect a folder as a data source to DataSQRL server",
  Long:  `Connect a folder as a data source to DataSQRL server`,
  Example: "datasqrl connect source folder /some/folder",
  Args: cobra.ExactArgs(1),
  RunE: func(cmd *cobra.Command, args []string) error {
    uri := args[0]

    //See if this is a local file or directory and if so, make it absolute
    info, err := os.Stat(uri)
    if err==nil && info.IsDir() {
      absPath, err := filepath.Abs(uri)
      if err != nil {
        return err
      }
      uri = absPath //need to turn into uri: "file://"+
    }

    payload := api.Payload {
      "uri": uri,
    }
    name := viper.GetString("name")
    if len(name)>0 {
      payload["name"]=name
    }
    resource := "/source/file"
    if verbose {
      cmd.Printf("Posting payload [%s] to resource [%s]",payload, resource)
    }
    result, err := api.Post2API(clientConfig, resource, payload)
    if err != nil {
      return err
    } else {
      cmd.Println(result)
      return nil
    }
  },
}

var connectSourceKafkaCmd = &cobra.Command{
  Use:   "kafka [uri]",
  Short: "Connect a Kafka log as a data source to DataSQRL server",
  Long:  `Connect a Kafka log as a data source to DataSQRL server`,
  Example: "datasqrl connect source kafka 10.20.20.10",
  Args: cobra.ExactArgs(1),
  RunE: func(cmd *cobra.Command, args []string) error {
    cmd.Println("Connect source kafka")
    return nil
  },
}

var connectSinkCmd = &cobra.Command{
  Use:   "sink",
  Short: "Connect a sink to DataSQRL server",
  Long:  `Connect a sink to DataSQRL server`,
}

var connectSinkFolderCmd = &cobra.Command{
  Use:   "folder [uri]",
  Short: "Connect a folder as a data sink to DataSQRL server",
  Long:  `Connect a folder as a data sink to DataSQRL server`,
  Example: "datasqrl connect sink folder /some/folder",
  Args: cobra.ExactArgs(1),
  RunE: func(cmd *cobra.Command, args []string) error {
    cmd.Println("Connect sink folder")
    return nil
  },
}

var connectSinkKafkaCmd = &cobra.Command{
  Use:   "kafka [uri]",
  Short: "Connect a Kafka log as a data sink to DataSQRL server",
  Long:  `Connect a Kafka log as a data sink to DataSQRL server`,
  Example: "datasqrl connect sink kafka 10.20.20.10",
  Args: cobra.ExactArgs(1),
  RunE: func(cmd *cobra.Command, args []string) error {
    cmd.Println("Connect sink kafka")
    return nil
  },
}
