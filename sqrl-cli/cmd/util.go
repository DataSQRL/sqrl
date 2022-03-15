package cmd

import (
  "strings"
  "errors"
  "path/filepath"
  "io/ioutil"
)

func fileNameWithoutExtension(fileName string) string {
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}

func readFileContent(fileName string) (string, error) {
  content, err := ioutil.ReadFile(fileName)
  if err != nil {
    return "", err
  }
  if content == nil || len(content)==0 {
    return "", errors.New("File is empty")
  }
  return string(content), nil
}
