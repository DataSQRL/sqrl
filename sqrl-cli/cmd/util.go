package cmd

import (
  "fmt"
  "strings"
  "errors"
  "path/filepath"
  "io/ioutil"
  "runtime"
  "os/exec"

  "github.com/DataSQRL/datasqrl/cli/pkg/api"
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
    return "", errors.New("File ["+fileName+"] is empty")
  }
  return string(content), nil
}

func containsIgnoreCase(items []string, item string) bool {
  for _, i := range items {
    if strings.EqualFold(item,i) {
      return true
    }
  }
  return false
}

func get2StringFctByKeys(keys []string) func (payload api.Payload) string {
  return func(payload api.Payload) string {
    return payload2StringByKeys(payload, keys)
  }
}

func payload2StringByKeys(payload api.Payload, keys []string) string {
  result := ""
  for _, key := range keys {
    result += key + ": " + fmt.Sprint(payload[key]) + "\n"
  }
  return result
}

func payload2StringTopLevel(payload api.Payload) string {
  var keys []string
  for key, v := range payload {
    switch v.(type) {
    case api.Payload:
      continue
    default:
      keys = append(keys,key)
    }
  }
  return payload2StringByKeys(payload, keys)
}

func openURL(url string) error {
    var cmd string
    var args []string

    switch runtime.GOOS {
    case "windows":
        cmd = "cmd"
        args = []string{"/c", "start"}
    case "darwin":
        cmd = "open"
    default: // "linux", "freebsd", "openbsd", "netbsd"
        cmd = "xdg-open"
    }
    args = append(args, url)
    return exec.Command(cmd, args...).Start()
}

func statusEqualsSuccess(payload api.Payload) bool {
  return strings.EqualFold("success",payload["status"].(string))
}
