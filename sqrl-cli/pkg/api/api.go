package api

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/PuerkitoBio/purell"
	"io"
	"io/ioutil"
	"net/http"
)

type Payload map[string]interface{}

type ClientConfig struct {
	AdminUrl string
	QueryUrl string
	Insecure bool
}

func GetFromAPI(cfg *ClientConfig, resource string) (Payload, error) {
	body, err := getCall(cfg, resource)
	if err != nil {
		return nil, err
	}
	return response2JsonObject(body)
}

func GetMultipleFromAPI(cfg *ClientConfig, resource string) ([]Payload, error) {
	body, err := getCall(cfg, resource)
	if err != nil {
		return nil, err
	}
	return response2JsonArray(body)
}

func getCall(cfg *ClientConfig, resource string) ([]byte, error) {
	apiURL, err := purell.NormalizeURLString(cfg.AdminUrl+resource, purell.FlagsUsuallySafeGreedy)
	println(apiURL)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest("GET", apiURL, nil)
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")

	return executeRequest(cfg, request)
}

func Post2API(cfg *ClientConfig, resource string, payload Payload) (Payload, error) {
	apiURL, err := purell.NormalizeURLString(cfg.AdminUrl+resource, purell.FlagsUsuallySafeGreedy)
	if err != nil {
		return nil, err
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(jsonData))
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")

	body, err := executeRequest(cfg, request)
	if err != nil {
		return nil, err
	}
	return response2JsonObject(body)
}

func response2JsonObject(body []byte) (Payload, error) {
	var result Payload
	err := json.Unmarshal(body, &result)

	switch {
	case err == io.EOF:
		return nil, nil
	case err != nil:
		return nil, err
	}
	return result, nil
}

func response2JsonArray(body []byte) ([]Payload, error) {
	var result []Payload
	err := json.Unmarshal(body, &result)

	switch {
	case err == io.EOF:
		return nil, nil
	case err != nil:
		return nil, err
	}
	return result, nil
}

func executeRequest(cfg *ClientConfig, request *http.Request) ([]byte, error) {
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
	body, err := ioutil.ReadAll(response.Body)

	if err != nil {
		return nil, err
	}
	return body, nil
}
