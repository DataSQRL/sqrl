package cmd

import (
  "io/ioutil"
  "path/filepath"
  "os"
  "strings"

  "github.com/spf13/cobra"
  "github.com/spf13/viper"

  "github.com/DataSQRL/datasqrl/cli/pkg/api"
)

func init() {
  rootCmd.AddCommand(deployCmd)
}

//TODO: read queries and schema (if exist)
var deployCmd = &cobra.Command{
  Use:   "deploy [script] [version]",
  Short: "Deploy SQRL script for execution on DataSQRL server",
  Long:  `Deploy the SQRL script (plus associated schema and query templates)
to DataSQRL server for execution`,
  Args: cobra.RangeArgs(1,2),
  Example: "datasqrl deploy example.sqrl v1",
  Run: func(cmd *cobra.Command, args []string) {
    fileName := args[0]

    version := ""
    if len(args)>1 {
      version = args[1]
    } else if verbose {
      cmd.Println("WARNING: no script version provided, using default version")
    }

    payload, hasSchema, err := assembleScriptBundle(fileName, version, true, cmd)
    if err != nil {
      cmd.PrintErrln(err)
    }

    if verbose {
      cmd.Println("Posting script bundle to resource [/deployment]")
    }
    deployment, err := api.Post2API(clientConfig, "/deployment", payload)
    if err != nil {
      cmd.PrintErrln(err)
    } else {
      success := printDeploymentResult(deployment, cmd)
      if !hasSchema && success {
        //Extract schema from compilation and store locally
        err := saveCompiledSchema(deployment["compilation"].(api.Payload), cmd)
        if err != nil {
          cmd.PrintErrln("Could not write pre-schema to file", err)
        }
      }
    }
  },
}

func printDeploymentResult(deployment api.Payload, cmd *cobra.Command) bool {
  status := deployment["status"].(string)
  deployId := deployment["id"].(string)
  compilation := deployment["compilation"].(api.Payload)

  failure := strings.EqualFold(status, "failed")
  if failure {
    cmd.PrintErrf("Failed deployment with id=%s\n", deployId)
  } else {
    cmd.Printf("Successful deployment with id=%s and status=%s\n", deployId, status)
  }
  printCompilationMessages(compilation, cmd)
  return !failure
}

func printCompilationMessages(deployment api.Payload, cmd *cobra.Command) {
  printMessages("ERROR", deployment["errors"].([]string), cmd)
  printMessages("WARN", deployment["warnings"].([]string), cmd)
  if verbose {
    printMessages("INFO", deployment["informations"].([]string), cmd)
  }
}

func printMessages(prefix string, messages []string, cmd *cobra.Command) {
  for _, msg := range messages {
    cmd.Println("[",prefix,"] ",msg)
  }
}

const defaultPreSchemaExt = ".yaml"

func saveCompiledSchema(compilation api.Payload, cmd *cobra.Command) error {
  schemaFileName := viper.GetString("schema")
  if (schemaFileName == globalFlags["schema"].defaultValue) {
    schemaFileName += defaultPreSchemaExt
  }
  schema := compilation["pre-schema"].(string)
  if len(schema)>0 {
    if verbose {
        cmd.Println("Writing compiled pre-schema to file: ", schemaFileName)
    }
    return ioutil.WriteFile(schemaFileName, []byte(schema), 0644)
  } else {
    if verbose {
        cmd.Println("No pre-schema was returned due to failed compilation")
    }
    return nil
  }
}

func getDeploymentName(fileName string) string {
  name := viper.GetString("name")
  if len(name)<1 {
    name = fileNameWithoutExtension(fileName)
  }
  return name
}

var graphQLExtensions = map[string]bool{"gql":true, "graphql":true}

func assembleScriptBundle(fileName string, version string, includeSchema bool,
                          cmd *cobra.Command) (api.Payload, bool, error) {
  //Read script content
  scriptContent, err := readFileContent(fileName)
  if err != nil {
    return nil, false, err
  }

  //Set Version
  name := getDeploymentName(fileName)

  if verbose {
    cmd.Printf("Deploying script [%s] with version [%s]\n",name,version)
  }

  baseDir := filepath.Dir(fileName)

  //Read pre-schema content (if any)
  schemaContent := ""
  hasSchema := false
  if (includeSchema) {
    schemaFileName := viper.GetString("schema")
    schemaContent, err = readFileContent(schemaFileName)
    if os.IsNotExist(err) {
      //Not a direct filename, try searching for it in script directory
      files, err := ioutil.ReadDir(baseDir)
      if err != nil {
        return nil, false, err
      }
      err = nil
      for _, file := range files {
        if !file.IsDir() && strings.EqualFold(fileNameWithoutExtension(file.Name()),schemaFileName) {
          schemaFileName = filepath.Join(baseDir,file.Name())
          schemaContent, err = readFileContent(schemaFileName)
          break
        }
      }
    }
    if (err != nil) {
      return nil, false, err
    }
    hasSchema = len(schemaContent)>0
    if verbose {
      if !hasSchema {
        cmd.Println("INFO: Did not find pre-schema file - submitting without")
      } else {
        cmd.Println("Reading pre-schema from file: ", schemaFileName)
      }
    }
  }

  //Read queries (if any)
  var queries = []api.Payload{}
  queryDir := viper.GetString("queries")
  files, err := ioutil.ReadDir(queryDir)
  if (err != nil) {
    queryDir = filepath.Join(baseDir, queryDir)
    files, err = ioutil.ReadDir(queryDir)
  }
  if err == nil {
    if verbose {
      cmd.Println("Reading queries from directory: ",queryDir)
    }
    for _, file := range files {
      if _, ok := graphQLExtensions[strings.ToLower(filepath.Ext(file.Name()))]; ok && !file.IsDir() {
        queryFile := filepath.Join(queryDir,file.Name())
        if verbose {
          cmd.Println("Adding query ", queryFile)
        }
        query, err := assembleQuery(queryFile)
        if err != nil {
          return nil, false, err
        }
        queries = append(queries,query)
      }
    }
  }


  script := api.Payload {
    "name": name,
    "script": scriptContent,
    "inputSchema": schemaContent,
    "isMain": true,
  }
  payload := api.Payload {
    "name": name,
    "version": version,
    "scripts": []api.Payload{script},
    "queries": queries,
  }
  return payload, hasSchema, nil
}

func assembleQuery(queryFile string) (api.Payload, error) {
  queryContent, err := readFileContent(queryFile)
  if err != nil {
    return nil, err
  }
  name := fileNameWithoutExtension(queryFile)
  return api.Payload {
    "name": name,
    "graphQL": queryContent,
  }, nil
}
