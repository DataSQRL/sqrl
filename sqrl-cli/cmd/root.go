package cmd

import (
  "fmt"
  "os"

  homedir "github.com/mitchellh/go-homedir"
  "github.com/spf13/cobra"
  "github.com/spf13/viper"
  "github.com/DataSQRL/datasqrl/cli/pkg/api"
)

var rootCmd = &cobra.Command{
  Use:   "datasqrl",
  Short: "DataSQRL: build data services quickly",
  Long: `Build data services quickly with DataSQRL -
check out https://datasqrl.com for more information`,
  PersistentPreRun: func(cmd *cobra.Command, args []string) {
    clientConfig = &api.ClientConfig{
      URL: viper.GetString(globalFlags["server"].name),
      Insecure : viper.GetBool(globalFlags["insecure"].name),
    }
    if verbose {
      cmd.Printf("Client connection configuration: %v+ \n",clientConfig)
    }
  },
}

func Execute() {
  if err := rootCmd.Execute(); err != nil {
    fmt.Println(err)
    os.Exit(1)
  }
}

type globalFlag struct {
  name string
  shortForm bool
  defaultValue interface{}
  description string
}

var globalFlags = map[string]*globalFlag {
  "server": &globalFlag{
    name: "server", shortForm: true, defaultValue: "localhost:5070",
    description: "IP or URL of DataSQRL server to connect to",
  },
  "insecure": &globalFlag{
    name: "insecure", shortForm: false, defaultValue: false,
    description: "disables SSL validation - only use in secure development environments",
  },
  "queries": &globalFlag{
    name: "queries", shortForm: false, defaultValue: "./queries",
    description: "folder containing the query templates for the deployment",
  },
  "schema": &globalFlag{
    name: "schema", shortForm: false, defaultValue: "pre-schema",
    description: "filename of the pre-schema yaml file for the deployment",
  },
  "name": &globalFlag{
    name: "name", shortForm: true, defaultValue: "",
    description: "assigned name of deployment, source, or sink",
  },
}

const defaultConfigFileName = "datasqrl-cfg"

var cfgFile string
var verbose bool
var clientConfig *api.ClientConfig

func init() {
  cobra.OnInitialize(initConfig)
  rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is ./"+defaultConfigFileName+".yaml)")
  rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "enable verbose mode")

  for key, flag := range globalFlags {
    switch flagType := flag.defaultValue.(type) {
    case string:
      if flag.shortForm {
        rootCmd.PersistentFlags().StringP(flag.name, flag.name[0:1], flag.defaultValue.(string), flag.description)
      } else {
        rootCmd.PersistentFlags().String(flag.name, flag.defaultValue.(string), flag.description)
      }
    case bool:
      if flag.shortForm {
        rootCmd.PersistentFlags().BoolP(flag.name, flag.name[0:1], flag.defaultValue.(bool), flag.description)
      } else {
        rootCmd.PersistentFlags().Bool(flag.name, flag.defaultValue.(bool), flag.description)
      }
    case int:
      if flag.shortForm {
        rootCmd.PersistentFlags().IntP(flag.name, flag.name[0:1], flag.defaultValue.(int), flag.description)
      } else {
        rootCmd.PersistentFlags().Int(flag.name, flag.defaultValue.(int), flag.description)
      }
    default:
      fmt.Printf("Unexpected flag type encountered: %s", flagType);
      os.Exit(1)
    }
    viper.BindPFlag(key, rootCmd.PersistentFlags().Lookup(flag.name))
    viper.SetDefault(key, flag.defaultValue)
  }


}

func initConfig() {
  // Read config either from cfgFile or from local directory!
  if cfgFile != "" {
    viper.SetConfigFile(cfgFile)
  } else {
    // Find home directory.
    home, err := homedir.Dir()
    if err != nil {
      fmt.Println(err)
      os.Exit(1)
    }

    // Search config in home directory with name "datasqrl-cfg" (without extension).
    viper.AddConfigPath(home)
    viper.AddConfigPath(".")
    viper.SetConfigName(defaultConfigFileName)
    viper.SetConfigType("yaml")
  }

  viper.ReadInConfig() //Ignore errors since config file isn't required
}
