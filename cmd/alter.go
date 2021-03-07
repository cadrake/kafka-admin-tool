package cmd

import (
    "fmt"
    "strconv"

    "github.com/Shopify/sarama"
    "github.com/spf13/cobra"
)

var (
    newReplicationFactor int
    deltaReplicationFactor int
    
    alterCmd = &cobra.Command{
        Use: "alter",
        Short: "Commands for altering the configuration of topics",
        Long: "The alter command provides a way to change a topic partitions or other configuration settings",
        Args: cobra.NoArgs,
        PreRunE: func(cmd *cobra.Command, args []string) error {
            if newReplicationFactor == -1 && deltaReplicationFactor == 0 {
                return fmt.Errorf("One of --new-rf or --delta-rf must be provided")
            }
            return nil
        },
        Run: func(cmd *cobra.Command, args []string) {
            client.AlterConfigs(buildTopicAlterConfig())
        },
    }
)

func init() {
    alterCmd.Flags().IntVar(&newReplicationFactor, "new-rf", -1, "New replication factor value for topics")    
    alterCmd.Flags().IntVar(&deltaReplicationFactor, "delta-rf", 0, "Amount to increase/decrease replication factor of topics")
    
    rootCmd.AddCommand(alterCmd)
}

func buildTopicAlterConfig() sarama.AlterConfigsRequest {
    metadata := client.GetMetadata()
        
    request := sarama.AlterConfigsRequest{
        ValidateOnly: isDryRun,
        Resources:    []*sarama.AlterConfigsResource{},
    }

    for _, topicMeta := range metadata.Topics {
        if topicRe != nil && !topicRe.MatchString(topicMeta.Name) {
            continue
        }

        // TODO: This needs to also use partition reassignment
        var newFactor string
        if deltaReplicationFactor != 0 {
            // TODO: Get existing RF and apply delta
        } else {
            newFactor = strconv.Itoa(newReplicationFactor)
        }

        entries := make(map[string]*string)
        entries["replication.factor"] = &newFactor

        alterRes := &sarama.AlterConfigsResource{
            Type:          sarama.TopicResource,
            Name:          topicMeta.Name,
            ConfigEntries: entries,
        }

        request.Resources = append(request.Resources, alterRes)
    }

    return request
}
