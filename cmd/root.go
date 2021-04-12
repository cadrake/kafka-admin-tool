package cmd

import (
    "fmt"
    "regexp"

    log "github.com/sirupsen/logrus"
    "github.com/spf13/cobra"

    "cadrake/kafka-admin-tool/utils"
)

var (
    brokerList  string
    topicFilter string
    caCertFile  string
    doExecute    bool
    topicRe     *regexp.Regexp
    client      *utils.AdminClient

    rootCmd = &cobra.Command{
        Use:   "kafka-admin-tool",
        Short: "A utility for working with kafka partitions",
        Long:  "kafka-admin-tool allows for reassigning partitions and altering topics inside a Kafka cluster",
        PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
            if len(topicFilter) == 0 {
                return fmt.Errorf("Missing required flag --topic-filter")
            }

            // Build regular expression
            var err error
            if topicRe, err = regexp.Compile(topicFilter); err != nil {
                return err
            }

            log.Infof("Connecting to brokers: %s", brokerList)
            client = utils.NewAdminClient(brokerList, caCertFile)
            return nil
        },
        PersistentPostRun: func(cmd *cobra.Command, args []string) {
            if client != nil {
                client.Close()
            }
        },
    }
)

func Execute() {
    rootCmd.Execute()
}

func init() {
    rootCmd.PersistentFlags().StringVar(&brokerList, "broker-list", "localhost:9092", "Kafka brokers to connect to")
    rootCmd.PersistentFlags().StringVar(&topicFilter, "topic-filter", "", "Regular expression used to match topics to reassign (optional)")
    rootCmd.PersistentFlags().StringVar(&caCertFile, "cacert-file", "", "Location of ca certificate for ssl communication with cluster")
    rootCmd.PersistentFlags().BoolVar(&doExecute, "execute", false, "True to apply changes to the cluster")
}
