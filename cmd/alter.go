package cmd

import (
	"regexp"

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
		Args: cobra.NoArgs(),
		Run: func(cmd *cobra.Command, args []string) {
			// Build regular expression
			var topicRe *regexp.Regexp
			if len(topicFilter) > 0 {
				var err error
				topicRe, err = regexp.Compile(topicFilter)
				utils.LogAndExitIfError(logger, "Failed to compile topic filter regular expression", err)
			}

			alterTopics(topicRe)
		},
	}
)

func init() {
	alterCmd.Flags().IntVar(&newReplicationFactor, "new-rf", -1, "New replication factor value for topics")	
	alterCmd.Flags().IntVar(&deltaReplicationFactor, "delta-rf", 0, "Amount to increase/decrease replication factor of topics")
	
	rootCmd.AddCommand(alterCmd)
}

func alterTopics(topicRe *regexp.Regexp) {
	client.AlterConfigs(buildTopicAlterConfig(topicRe))
}
	
func buildTopicAlterConfig(topicRe *regexp.Regexp) sarama.AlterConfigsRequest {
	metadata := client.GetMetadata()
		
	request := sarama.AlterConfigsRequest{
		ValidateOnly: isDryRun,
		Resources:    []*sarama.AlterConfigsResource{},
	}

	for _, topicMeta := range metadata.Topics {
		if topicRe != nil && !topicRe.MatchString(topicMeta.Name) {
			continue
		}

		var newFactor string
		if deltaReplicationFactor != 0 {
			// TODO: Get existing RF and apply delta
		} else {
			newFactor = newReplicationFactor
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
