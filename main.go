package main

import (
	"fmt"
	"github.com/grepplabs/kafka-proxy/cmd/kafka-proxy"
	"github.com/spf13/cobra"
	"os"
)

var RootCmd = &cobra.Command{
	Use:   "kafka-proxy",
	Short: "Server that proxies requests to Kafka brokers",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
		os.Exit(1)
	},
}

func init() {
	RootCmd.AddCommand(server.Command)
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
