package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/DataSQRL/datasqrl/cli/pkg/api"
)

const developmentVersion = "v1"

const prepopulateFlag = "initialize"
const openBrowserFlag = "open"

func init() {
	rootCmd.AddCommand(watchCmd)
	watchCmd.PersistentFlags().BoolP(prepopulateFlag, prepopulateFlag[0:1], true, "initialize script with imports of all tables connected to server")
	viper.BindPFlag(prepopulateFlag, watchCmd.PersistentFlags().Lookup(prepopulateFlag))
	viper.SetDefault(prepopulateFlag, true)

	watchCmd.PersistentFlags().BoolP(openBrowserFlag, openBrowserFlag[0:1], true, "open default web browser to query API")
	viper.BindPFlag(openBrowserFlag, watchCmd.PersistentFlags().Lookup(openBrowserFlag))
	viper.SetDefault(openBrowserFlag, true)
}

var watchCmd = &cobra.Command{
	Use:   "watch [script]",
	Short: "Watch SQRL script for development",
	Long: `Watches provided SQRL script and continuously submits changes to DataSQRL
server for execution. Creates SQRL script if it does not exist.`,
	Args:    cobra.ExactArgs(1),
	Example: "datasqrl watch test.sqrl",
	RunE: func(cmd *cobra.Command, args []string) error {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			return nil
		}
		defer watcher.Close()

		fileName := args[0]

		_, err = os.Stat(fileName)
		if err != nil {
			//Create new script with imports pre-populated
			if verbose {
				cmd.Println("Creating script file: ", fileName)
			}
			imports := ""
			if viper.GetBool(prepopulateFlag) {
				sources, err := api.GetMultipleFromAPI(clientConfig, "/source")
				if err != nil {
					return err
				}
				numSources := 0
				numTables := 0
				for _, source := range sources {
					sourceName := source["sourceName"].(string)
					numSources++
					tables := source["tables"].([]interface{})
					for _, table := range tables {
						imports = imports + "IMPORT " + sourceName + "." + fmt.Sprint(table) + ";\n"
						numTables++
					}
				}
				if verbose {
					cmd.Printf("Initialize script file [%s] with %d data sources and %d tables\n", fileName, numSources, numTables)
				}
			}
			err := ioutil.WriteFile(fileName, []byte(imports), 0644)
			if err != nil {
				return err
			}
		}

		terminate := make(chan bool)

		go scriptUpdate(cmd, fileName, watcher)

		if viper.GetBool(openBrowserFlag) {
			openURL(clientConfig.QueryUrl + "/" + getDeploymentName(fileName) + "/" + developmentVersion)
		}

		err = watcher.Add(fileName)
		if err != nil {
			return err
		}
		<-terminate
		return nil
	},
}

func deployScript(cmd *cobra.Command, fileName string) time.Time {
	cmd.Println("Starting development deployment...")
	payload, _, err := assembleScriptBundle(fileName, "", false, cmd)
	if err != nil {
		cmd.PrintErrln("[ERROR] Could not assemble deployment", err)
		return time.Time{}
	}
	deployTime := time.Now()
	deployment, err := api.Post2API(clientConfig, "/deployment", payload)
	if err != nil {
		cmd.PrintErrln(err)
		return time.Time{}
	} else {
		printDeploymentResult(deployment, cmd)
		err := saveCompiledSchemas(deployment["compilation"].(map[string]interface{}), cmd)
		if err != nil {
			cmd.PrintErrln("Could not write pre-schema to file", err)
		}
		return deployTime
	}
}

func scriptUpdate(cmd *cobra.Command, fileName string, watcher *fsnotify.Watcher) {
	lastDeployTime := deployScript(cmd, fileName)
	if verbose {
		cmd.Printf("Watching script [%s] for changes to automatically deploy to server\n", fileName)
	}
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				cmd.PrintErrln("Could not retrieve filesystem notifications")
				return
			}
			cmd.Println("Received file event:", event)
			if event.Op&fsnotify.Write == fsnotify.Write {
				file, err := os.Stat(fileName)
				if err != nil {
					cmd.PrintErrln(err)
					return
				}
				lastModTime := file.ModTime()
				cmd.Println("Times", lastModTime, lastDeployTime)
				if lastDeployTime.Before(lastModTime) {
					lastDeployTime = deployScript(cmd, fileName)
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				cmd.PrintErrln("Could not retrieve filesystem notifications")
				return
			}
			cmd.PrintErrln("Encountered filesystem notification error: ", err)
		}
	}
}
