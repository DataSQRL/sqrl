package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/docker/docker/client"
	"golang.org/x/net/context"
)

// Set up global variables for the docker client and context
var (
	cli *client.Client
	ctx = context.Background()
)

func main() {
	// Initialize the Viper and Cobra libraries
	viper.AutomaticEnv()
	var rootCmd = &cobra.Command{
		Use:   "orchestrate",
		Short: "Orchestrate docker containers",
		Long:  `Orchestrate docker containers using Viper and Cobra`,
		Run: func(cmd *cobra.Command, args []string) {
			// Create the docker client
			cli, err := client.NewEnvClient()
			if err != nil {
				panic(err)
			}

			// Create a private network for the containers to communicate on
			networkResp, err := cli.NetworkCreate(ctx, "private-net", client.NetworkCreate{
				CheckDuplicate: true,
				Driver:         "bridge",
				Internal:       true,
			})
			if err != nil {
				panic(err)
			}
			fmt.Printf("Created network with ID: %s\n", networkResp.ID)

			// Create the postgres container and connect it to the private network
			postgresContainer, err := cli.ContainerCreate(ctx, &client.ContainerConfig{
				Image: "postgres",
				Env:   []string{"POSTGRES_PASSWORD=mysecretpassword"},
				ExposedPorts: map[string]struct{}{
					"5432/tcp": {},
				},
				HostConfig: &client.HostConfig{
					NetworkMode: "private-net",
				},
			}, nil, nil, "")
			if err != nil {
				panic(err)
			}
			fmt.Printf("Created postgres container with ID: %s\n", postgresContainer.ID)

			// Start the postgres container
			if err := cli.ContainerStart(ctx, postgresContainer.ID, types.ContainerStartOptions{}); err != nil {
				panic(err)
			}
			fmt.Println("Started postgres container")

			// Create the Compiler container and connect it to the private network
			compilerContainer, err := cli.ContainerCreate(ctx, &client.ContainerConfig{
				Image: "compiler",
				Env:   []string{"DB_HOST"}
				ExposedPorts: map[string]struct{}{
        					"5432/tcp": {},
        				},
        				HostConfig: &client.HostConfig{
        					NetworkMode: "private-net",
        				},
        			}, nil, nil, "")
		// Start the Compiler container
		if err := cli.ContainerStart(ctx, compilerContainer.ID, types.ContainerStartOptions{}); err != nil {
			panic(err)
		}
		fmt.Println("Started Compiler container")

		// Create the Executor container and connect it to the private network
		executorContainer, err := cli.ContainerCreate(ctx, &client.ContainerConfig{
			Image: "executor",
			Env:   []string{"DB_HOST=postgres", "DB_PASSWORD=mysecretpassword"},
			HostConfig: &client.HostConfig{
				NetworkMode: "private-net",
			},
		}, nil, nil, "")
		if err != nil {
			panic(err)
		}
		fmt.Printf("Created Executor container with ID: %s\n", executorContainer.ID)

		// Start the Executor container
		if err := cli.ContainerStart(ctx, executorContainer.ID, types.ContainerStartOptions{}); err != nil {
			panic(err)
		}
		fmt.Println("Started Executor container")
	},
}

if err := rootCmd.Execute(); err != nil {
	fmt.Println(err)
	os.Exit(1)
}
