package api

import (
	"errors"
	"net/http"
  "bytes"
	"encoding/json"
  "io/ioutil"
  "crypto/tls"
  "io"
	"github.com/PuerkitoBio/purell"

)

type Payload map[string]interface{}

type ClientConfig struct {
  URL string
  Insecure bool
}

func GetFromAPI(cfg *ClientConfig, resource string) (Payload, error) {
  response, err := getCall(cfg, resource)
  if err != nil {
    return nil, err
  }
  return response2JsonObject(response)
}

func GetMultipleFromAPI(cfg *ClientConfig, resource string) ([]Payload, error) {
  response, err := getCall(cfg, resource)
  if err != nil {
    return nil, err
  }
  return response2JsonArray(response)
}

func getCall(cfg *ClientConfig, resource string) (*http.Response, error) {
  url, err := purell.NormalizeURLString(cfg.URL + resource, purell.FlagsUsuallySafeGreedy)
  if err != nil {
    return nil, err
  }
  request, err := http.NewRequest("GET", url, nil)
  request.Header.Set("Content-Type", "application/json; charset=UTF-8")

  return executeRequest(cfg, request)
}

func Post2API(cfg *ClientConfig, resource string, payload Payload) (Payload, error) {
  url, err := purell.NormalizeURLString(cfg.URL + resource, purell.FlagsUsuallySafeGreedy)
  if err != nil {
    return nil, err
  }
  jsonData, err := json.Marshal(payload)
  if err != nil {
    return nil, err
  }
  request, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")

  response, err := executeRequest(cfg, request)
  if err != nil {
    return nil, err
  }
  return response2JsonObject(response)
}

func response2JsonObject(response *http.Response) (Payload, error) {
  var result Payload
  err := json.NewDecoder(response.Body).Decode(result)

  switch {
  case err == io.EOF:
    return nil, nil
  case err != nil:
    return nil, err
  }
  return result, nil
}

func response2JsonArray(response *http.Response) ([]Payload, error) {
  var result []Payload
  err := json.NewDecoder(response.Body).Decode(result)

  switch {
  case err == io.EOF:
    return nil, nil
  case err != nil:
    return nil, err
  }
  return result, nil
}

func executeRequest(cfg *ClientConfig, request *http.Request) (*http.Response, error) {
  var client *http.Client
  if cfg.Insecure {
    tlsConfig := &tls.Config{InsecureSkipVerify: true}
    transport := &http.Transport{TLSClientConfig: tlsConfig}
    client = &http.Client{Transport: transport}
  } else {
    client = &http.Client{}
  }

  response, err := client.Do(request)
  if err != nil {
    return nil, err
  }
  defer response.Body.Close()

  if response.StatusCode != 200 {
    message, err := ioutil.ReadAll(response.Body)
    if err != nil {
      return nil, err
    }
    return nil, errors.New("Error Calling Server API:\n" + string(message))
  }
  return response, nil;
}
