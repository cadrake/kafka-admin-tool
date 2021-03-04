package cmd

import (
	"log"
	
	"github.com/spf13/cobra"

	"github.com/cadrake/kafka-admin-tools/utils"
)

var (
	brokerList string
	topicFilter string
	caCertFile string
	isDryRun bool
	client utils.AdminClient
	logger *log.Logger
	
	rootCmd = &cobra.Command{
		Use: "kadmin",
		Short: "A utility for working with kafka partitions",
		Long: "kafka-admin-tool allows for reassigning partitions and altering topics inside a Kafka cluster",
	}
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logger.Fatal(err)
	}
}

func init() {
	logger := log.New(os.Stdout, "[kafka-admin-tool]", log.LstdFlags)

	rootCmd.PersistentFlags().StringVarP(&brokerList, "broker-list", "b", "localhost:9092", "Kafka brokers to connect to")
	rootCmd.PersistentFlags().StringVarP(&topicFilter, "topic-filter", "f", "", "Regular expression used to match topics to reassign (optional)")
	rootCmd.PersistentFlags().StringVar(&caCertFile, "cacert", nil, "Location of ca certificate for ssl communication with cluster")
	rootCmd.PersistentFlags().BoolVar(&isDryRun, "execute", false, "True to apply changes to the cluster")
}
