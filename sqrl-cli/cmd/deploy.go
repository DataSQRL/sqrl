package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/DataSQRL/datasqrl/cli/pkg/api"
)

func init() {
	rootCmd.AddCommand(deployCmd)
}

var deployCmd = &cobra.Command{
	Use:   "deploy [script] [version]",
	Short: "Deploy SQRL script for execution on DataSQRL server",
	Long: `Deploy the SQRL script (plus associated schema and query templates)
to DataSQRL server for execution`,
	Args:    cobra.RangeArgs(1, 2),
	Example: "datasqrl deploy test.sqrl v1",
	RunE: func(cmd *cobra.Command, args []string) error {
		fileName := args[0]

		version := ""
		if len(args) > 1 {
			version = args[1]
		} else if verbose {
			cmd.Println("WARNING: no script version provided, using default version")
		}

		payload, hasSchema, err := assembleScriptBundle(fileName, version, true, cmd)
		if err != nil {
			return err
		}

		if verbose {
			cmd.Println("Posting script bundle to resource [/deployment]")
		}
		deployment, err := api.Post2API(clientConfig, "/deployment", payload)
		if err != nil {
			return err
		} else {
			printDeploymentResult(deployment, cmd)
			if !hasSchema {
				//Extract schema from compilation and store locally
				err := saveCompiledSchemas(deployment["compilation"].(map[string]interface{}), cmd)
				if err != nil {
					return err
				}
			}
			return nil
		}
	},
}

func printDeploymentResult(deployment api.Payload, cmd *cobra.Command) {
	status := deployment["status"].(string)
	deployId := deployment["id"].(string)
	compilationResult := deployment["compilation"].(map[string]interface{})

	failure := strings.EqualFold(status, "failed")
	if failure {
		cmd.PrintErrf("Failed deployment with id=%s\n", deployId)
	} else {
		cmd.Printf("Successful deployment with id=%s and status=%s\n", deployId, status)
	}
	printCompilationResult(compilationResult, cmd)
}

func printCompilationResult(compilationResult map[string]interface{}, cmd *cobra.Command) {
	success := statusEqualsSuccess(compilationResult)
	compiletime := compilationResult["compileTime"].(float64)
	if success {
		cmd.Printf("Successful compilation took %6.0f ms\n", compiletime)
	} else {
		cmd.PrintErrf("Failed compilation took %6.0f ms - see below for compilation errors\n", compiletime)
	}
	for _, compilation := range compilationResult["compilations"].([]interface{}) {
		printCompilation(compilation.(map[string]interface{}), cmd)
	}
}

func printCompilation(compilation map[string]interface{}, cmd *cobra.Command) {
	// success := statusEqualsSuccess(compilation)
	fileName := compilation["filename"].(string)
	messages := compilation["messages"].([]interface{})

	errorMsgs, errorCount := messages2String(messages, "error", "[ERROR]")
	warnMsgs, warnCount := messages2String(messages, "warning", "[WARN]")
	infoMsgs, infoCount := messages2String(messages, "information", "[INFO]")

	cmd.Printf("#%s - %d errors, %d warnings, %d informations \n", fileName, errorCount, warnCount, infoCount)
	cmd.Print(errorMsgs)
	cmd.Print(warnMsgs)
	if verbose {
		cmd.Print(infoMsgs)
	}
}

func messages2String(compileMsgs []interface{}, filterType string, prefix string) (string, int) {
	result := ""
	count := 0
	for _, m := range compileMsgs {
		msg := m.(map[string]interface{})
		typeName := msg["type"].(string)
		if strings.EqualFold(typeName, filterType) {
			result += prefix + " " + fmt.Sprint(msg["location"]) + ": " + fmt.Sprint(msg["message"]) + "\n"
			count++
		}
	}
	return result, count
}

func printMessages(prefix string, messages []string, cmd *cobra.Command) {
	for _, msg := range messages {
		cmd.Println("[", prefix, "] ", msg)
	}
}

func saveCompiledSchemas(compilationResult map[string]interface{}, cmd *cobra.Command) error {
	success := statusEqualsSuccess(compilationResult)
	if !success {
		if verbose {
			cmd.Println("Compilation failed - not saving compiled schemas")
		}
		return nil
	}
	for _, compilation := range compilationResult["compilations"].([]interface{}) {
		err := saveCompiledSchema(compilation.(map[string]interface{}), cmd)
		if err != nil {
			return err
		}
	}
	return nil
}

func saveCompiledSchema(compilation map[string]interface{}, cmd *cobra.Command) error {
	//only script artifacts return a compiled schema
	artifactType := compilation["type"].(string)
	if !strings.EqualFold(artifactType, "script") {
		return nil
	}

	success := statusEqualsSuccess(compilation)
	fileName := compilation["filename"].(string)
	if !success {
		if verbose {
			cmd.Println("No pre-schema was returned due to failed compilation: ", fileName)
		}
		return nil
	}
	schemaFileName := getSchemaFileName(fileName)
	schema := compilation["preschema"].(string)
	if len(schema) > 0 {
		if verbose {
			cmd.Println("Writing compiled pre-schema for [", fileName, "] to file: ", schemaFileName)
		}
		return ioutil.WriteFile(schemaFileName, []byte(schema), 0644)
	} else {
		cmd.PrintErrln("No pre-schema returned for: ", fileName)
		return nil
	}
}

func getDeploymentName(fileName string) string {
	name := viper.GetString("name")
	if len(name) < 1 {
		name = fileNameWithoutExtension(filepath.Base(fileName))
	}
	return name
}

func getSchemaFileName(fileName string) string {
	schemaExtension := viper.GetString("schema")
	return fileNameWithoutExtension(fileName) + "." + schemaExtension
}

var graphQLExtensions = map[string]bool{"gql": true, "graphql": true}

func assembleScriptBundle(fileName string, version string, includeSchema bool,
	cmd *cobra.Command) (api.Payload, bool, error) {

	//Get deployment name and base
	name := getDeploymentName(fileName)
	scriptDir := filepath.Dir(fileName)

	if verbose {
		cmd.Printf("Deploying script [%s] with version [%s]\n", name, version)
	}

	//Read scripts
	//Currently, we are only putting the main script in the bundle
	//In the future, we want to add all .sqrl scripts in the directory scriptDir
	var scripts = []api.Payload{}
	script, hasSchema, err := assembleScript(fileName, true, includeSchema)
	if verbose {
		cmd.Printf("Adding main script [%s]", fileName)
		if !hasSchema {
			cmd.Println(" without schema")
		} else {
			cmd.Println(" with schema: ", getSchemaFileName(fileName))
		}
	}
	scripts = append(scripts, script)

	//Read queries (if any)
	var queries = []api.Payload{}
	queryDir := viper.GetString("queries")
	files, err := ioutil.ReadDir(queryDir)
	if err != nil {
		queryDir = filepath.Join(scriptDir, queryDir)
		files, err = ioutil.ReadDir(queryDir)
	}
	if err == nil {
		if verbose {
			cmd.Println("Reading queries from directory: ", queryDir)
		}
		for _, file := range files {
			if _, ok := graphQLExtensions[strings.ToLower(filepath.Ext(file.Name()))]; ok && !file.IsDir() {
				queryFile := filepath.Join(queryDir, file.Name())
				if verbose {
					cmd.Println("Adding query ", queryFile)
				}
				query, err := assembleQuery(queryFile)
				if err != nil {
					return nil, false, err
				}
				queries = append(queries, query)
			}
		}
	}

	payload := api.Payload{
		"name":    name,
		"version": version,
		"scripts": scripts,
		"queries": queries,
	}
	return payload, hasSchema, nil
}

func assembleScript(scriptFile string, isMain bool, includeSchema bool) (api.Payload, bool, error) {
	//Read script content
	scriptContent, err := readFileContent(scriptFile)
	if err != nil {
		return nil, false, err
	}
	name := fileNameWithoutExtension(filepath.Base(scriptFile))

	//Read pre-schema content (if any)
	schemaContent := ""
	hasSchema := false
	if includeSchema {
		schemaFileName := getSchemaFileName(scriptFile)
		schemaContent, err = readFileContent(schemaFileName)
		if err != nil {
			if os.IsNotExist(err) {
				schemaContent = ""
			} else {
				return nil, false, err
			}
		}
		hasSchema = len(schemaContent) > 0
	}

	script := api.Payload{
		"name":        name,
		"filename":    scriptFile,
		"content":     scriptContent,
		"inputSchema": schemaContent,
		"main":        isMain,
	}
	return script, hasSchema, nil
}

func assembleQuery(queryFile string) (api.Payload, error) {
	queryContent, err := readFileContent(queryFile)
	if err != nil {
		return nil, err
	}
	name := fileNameWithoutExtension(filepath.Base(queryFile))
	return api.Payload{
		"name":     name,
		"filename": queryFile,
		"graphQL":  queryContent,
	}, nil
}
