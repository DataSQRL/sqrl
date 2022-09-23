package cmd

import (
	"fmt"
	"github.com/DataSQRL/datasqrl/cli/pkg/api"
	"github.com/fatih/color"
)

func DisplayError(results []api.Payload) {

	// error message - text color based on severity

	for _, result := range results {

		switch result["severity"] {
		case "fatal":
			c := color.New(color.FgHiRed, color.Bold)
			c.Println("Error: " + result["message"].(string))
		case "warning":
			c := color.New(color.FgHiYellow, color.Bold)
			c.Println("Warning: " + result["message"].(string))
		case "information":
			c := color.New(color.FgBlue, color.Bold)
			c.Println("Info: " + result["message"].(string))
		}

		// file path, line, and offset

		if result["location"] != nil {

			fmt.Printf("At: %v%v %v:%v\n",
				result["location"].(map[string]interface{})["prefix"],
				result["location"].(map[string]interface{})["path"],
				result["location"].(map[string]interface{})["file"].(map[string]interface{})["line"],
				result["location"].(map[string]interface{})["file"].(map[string]interface{})["offset"])
		}

		// highlighting

		if result["location"].(map[string]interface{})["file"].(map[string]interface{})["context"] != nil {

			hStart := int(result["location"].(map[string]interface{})["file"].(map[string]interface{})["context"].(map[string]interface{})["highlight_start"].(float64))
			hEnd := int(result["location"].(map[string]interface{})["file"].(map[string]interface{})["context"].(map[string]interface{})["highlight_end"].(float64))
			text := result["location"].(map[string]interface{})["file"].(map[string]interface{})["context"].(map[string]interface{})["text"].(string)

			fmt.Print(text[:hStart])
			h := color.New(color.FgHiYellow, color.Bold, color.Underline)
			h.Print(text[hStart:hEnd])
			fmt.Print(text[hEnd:] + "\n")
		}
	}
}
