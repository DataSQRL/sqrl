package cmd

import (
  "fmt"
  "os"

  homedir "github.com/mitchellh/go-homedir"
  "github.com/spf13/cobra"
  "github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
  Use:   "datasqrl",
  Short: "DataSQRL: build data services quickly",
  Long: `Build data services quickly with DataSQRL -
check out https://datasqrl.com for more information`,
  Run: func(cmd *cobra.Command, args []string) {
    fmt.Println("Running root command: " + viper.GetString(serverFlag))
  },
}

func Execute() {
  if err := rootCmd.Execute(); err != nil {
    fmt.Println(err)
    os.Exit(1)
  }
}


const serverFlag = "server"

const defaultConfigFileName = "datasqrl-cfg"
const defaultServerIP = "localhost:5070"

var cfgFile string

func init() {
  cobra.OnInitialize(initConfig)
  rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./"+defaultConfigFileName+".yaml)")
  rootCmd.PersistentFlags().StringP(serverFlag, serverFlag[0:1], "", "IP or URL of DataSQRL server to connect to")

  // viper.BindPFlag("author", rootCmd.PersistentFlags().Lookup("author"))
  viper.BindPFlag(serverFlag, rootCmd.PersistentFlags().Lookup(serverFlag))
  viper.SetDefault(serverFlag, defaultServerIP)
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

  viper.ReadInConfig()
}
