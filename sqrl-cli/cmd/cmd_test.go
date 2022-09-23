package cmd

import (
	"bytes"
	"io/ioutil"
	// "fmt"
	"os"
	"strings"
	"testing"
)

const scriptFile = "script.sqrl"
const schemaFile = "script.pre-schema.yaml"

const sourceName = "testdata"

//var dir = "./"

func TestCmds(t *testing.T) {
	var res string
	cleanup()
	defer cleanup()
	//This functions assumes a running (and empty) DataSQRL server instance
	//
	//connectSourceFolderCmd.SetArgs([]string{"connect", "source", "folder", dir})
	//connectSourceFolderCmd.Execute()
	//
	//Reading empty sources

	res = executeCommand([]string{"list", "sources"}, t)
	if !strings.HasPrefix(res, "Received 0 results") {
		t.Fatal("Failed to read empty sources, got result", res)
	}

	//Adding folder source
	sourceDir := "../../sqml-core/src/test/resources/data"
	res = executeCommand([]string{"connect", "source", "folder", sourceDir, "-n " + sourceName}, t)
	// fmt.Println("Test result")
	// fmt.Print(res)
	res = executeCommand([]string{"list", "sources"}, t)
	if !strings.HasPrefix(res, "---Result 1") {
		t.Fatal("Failed to read source back, got result", res)
	}

	res = executeCommand([]string{"list", "deployments"}, t)
	if !strings.HasPrefix(res, "Received 0 results") {
		t.Fatal("Failed to read empty deployments, got result", res)
	}
	writeScript("IMPORT testdata.book", t)
	res = executeCommand([]string{"deploy", scriptFile, "v1.0"}, t)
	res = executeCommand([]string{"list", "deployments"}, t)
	if !strings.HasPrefix(res, "---Result 1") {
		t.Fatal("Failed to read deployment back, got result", res)
	}

}

func cleanup() {
	os.Remove(scriptFile)
	os.Remove(schemaFile)
}

func writeScript(content string, t *testing.T) {
	err := ioutil.WriteFile(scriptFile, []byte(content), 0644)
	if err != nil {
		t.Fatal(err)
	}
}

func executeCommand(args []string, t *testing.T) string {
	b := bytes.NewBufferString("")
	rootCmd.SetOut(b)
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	res, err := readString(b)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func readString(buffer *bytes.Buffer) (string, error) {
	out, err := ioutil.ReadAll(buffer)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
