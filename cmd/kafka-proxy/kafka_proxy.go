package server

import (
	"errors"
	"fmt"
	"github.com/grepplabs/kafka-proxy/proxy"
	"github.com/spf13/cobra"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	defaultListenIP         string
	inFlightRequests        int
	bootstrapServersMapping = make([]string, 0)
)

var Command = &cobra.Command{
	Use:   "server",
	Short: "Run the kafka-proxy server",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if inFlightRequests < 1 {
			return fmt.Errorf("in-flight-requests must be positiv")
		}
		if defaultListenIP == "" {
			return fmt.Errorf("default-listen-ip must not be empty")
		}
		if net.ParseIP(defaultListenIP) == nil {
			return fmt.Errorf("default-listen-ip is not a valid IP")
		}
		return nil
	},
	RunE: RunE,
}

func init() {
	Command.Flags().StringVar(&defaultListenIP, "default-listen-ip", "127.0.0.1", "Default listen IP")
	Command.Flags().IntVar(&inFlightRequests, "in-flight-requests", 256, "Number of in-flight requests pro tcp connection")
	Command.Flags().StringArrayVar(&bootstrapServersMapping, "bootstrap-server-mapping", []string{}, "Mapping of Kafka bootstrap server address to local address (host:port,host:port)")
	Command.MarkFlagRequired("bootstrap-server-mapping")
}

func RunE(_ *cobra.Command, _ []string) error {
	// All active connections are stored in this variable.
	connset := proxy.NewConnSet()

	// build/kafka-proxy server --bootstrap-server-mapping "192.168.99.100:32400,0.0.0.0:32399" \
	//  --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32400" \
	//  --bootstrap-server-mapping "192.168.99.100:32401,127.0.0.1:32401" \
	//  --bootstrap-server-mapping "192.168.99.100:32402,127.0.0.1:32402"

	bootstrapCfg, err := getListenConfigs(bootstrapServersMapping)
	if err != nil {
		return err
	}

	listeners := proxy.NewListeners(defaultListenIP)
	connSrc, err := listeners.ListenInstances(bootstrapCfg)
	if err != nil {
		log.Fatal(err)
	}

	processorConfig := proxy.ProcessorConfig{
		InFlightRequests:      inFlightRequests,
		NetAddressMappingFunc: listeners.GetNetAddressMapping,
	}
	log.Print("Ready for new connections")

	//stopChan <-chan struct{}

	stopChan := make(chan struct{}, 1)
	go handleSigterm(stopChan)

	(&proxy.Client{
		Conns:           connset,
		ProcessorConfig: processorConfig,
	}).Run(stopChan, connSrc)

	return nil
}

func getListenConfigs(bootstrapServersMapping []string) ([]proxy.ListenConfig, error) {
	if len(bootstrapServersMapping) == 0 {
		return nil, errors.New("list of bootstrap-server-mapping must not be empty")
	}
	listenConfigs := make([]proxy.ListenConfig, 0)
	for _, v := range bootstrapServersMapping {
		pair := strings.Split(v, ",")
		if len(pair) != 2 {
			return nil, errors.New("bootstrap-server-mapping must be in form 'remotehost:remoteport,localhost:localport'")
		}
		remotehost, remoteport, err := proxy.SplitHostPort(pair[0])
		if err != nil {
			return nil, err
		}
		localhost, localport, err := proxy.SplitHostPort(pair[1])
		if err != nil {
			return nil, err
		}
		listenConfig := proxy.ListenConfig{BrokerAddress: net.JoinHostPort(remotehost, fmt.Sprint(remoteport)), ListenAddress: net.JoinHostPort(localhost, fmt.Sprint(localport))}
		listenConfigs = append(listenConfigs, listenConfig)
	}
	return listenConfigs, nil
}

func handleSigterm(stopChan chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	log.Print("Received terminating signal ...")
	close(stopChan)
}
