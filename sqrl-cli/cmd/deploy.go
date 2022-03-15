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
    } else {
      cmd.Println("WARNING: no script version provided, using default version")
    }

    payload := assembleScriptBundle(fileName, version, true, cmd)
    result, err := api.Post2API(serverConfig, "/deployment", payload)
    if err != nil {
      cmd.PrintErrln(err)
    } else {
      cmd.Println(result)
    }
  },
}


var graphQLExtensions = map[string]bool{"gql":true, "graphql":true}

func assembleScriptBundle(fileName string, version string, includeSchema bool,
                          cmd *cobra.Command) api.Payload {
  //Read script content
  scriptContent, err := readFileContent(fileName)
  if err != nil {
    cmd.PrintErrf("Could not read file [%s]: %s", fileName, err)
    os.Exit(1)
  }

  //Set Version
  name := viper.GetString("name")
  if len(name)<1 {
    name = fileNameWithoutExtension(fileName)
  }

  baseDir := filepath.Dir(fileName)

  //Read pre-schema content (if any)
  schemaContent := ""
  if (includeSchema) {
    schemaFileName := viper.GetString("schema")
    schemaContent, err = readFileContent(schemaFileName)
    if os.IsNotExist(err) {
      //Not a direct filename, try searching for it in script directory
      files, err := ioutil.ReadDir(baseDir)
      if err != nil {
        cmd.PrintErrf("Could not read script directory [%s]: %s", baseDir, err)
        os.Exit(1)
      }
      err = nil
      for _, file := range files {
        if !file.IsDir() && strings.ToLower(fileNameWithoutExtension(file.Name())) ==
                            strings.ToLower(schemaFileName) {
          schemaFileName = filepath.Join(baseDir,file.Name())
          schemaContent, err = readFileContent(schemaFileName)
          break
        }
      }
    }
    if (err != nil) {
      cmd.PrintErrf("Could not read pre-schema file [%s]: %s", schemaFileName, err)
      os.Exit(1)
    }
    if verbose && len(schemaContent)==0 {
      cmd.Println("INFO: Did not find pre-schema file - submitting without")
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
    for _, file := range files {
      if _, ok := graphQLExtensions[strings.ToLower(filepath.Ext(file.Name()))]; ok && !file.IsDir() {
        queryFile := filepath.Join(queryDir,file.Name())
        query, err := assembleQuery(queryFile)
        if err != nil {
          cmd.PrintErrf("Could not read query [%s]: %s",queryFile, err)
          os.Exit(1)
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
  return payload
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
