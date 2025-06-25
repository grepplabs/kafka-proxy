package main

import (
	"fmt"
	"github.com/grepplabs/kafka-proxy/cmd/kafka-proxy"
	"github.com/grepplabs/kafka-proxy/cmd/tools"
	"github.com/spf13/cobra"
	"log"
	"net/http"
	"os"
	"runtime"

	_ "net/http/pprof"
)

var RootCmd = &cobra.Command{
	Use:   "kafka-proxy",
	Short: "Server that proxies requests to Kafka brokers",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
		os.Exit(1)
	},
}

func init() {
	RootCmd.AddCommand(server.Server)
	RootCmd.AddCommand(server.Version)
	RootCmd.AddCommand(tools.Tools)
}

func main() {
	runtime.SetBlockProfileRate(1)     // blocking sync primitives
	runtime.SetMutexProfileFraction(1) // contended mutexes

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
