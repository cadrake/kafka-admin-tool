package cmd

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"regexp"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"

	"github.com/cadrake/kafka-admin-tool/utils"
)

var (
	fromBroker int
	toBroker int
	outputJson string
	
	reassignCmd = &cobra.Command{
		Use: "reassign",
		Short: "Reassign partitions in a cluster",
		Long: "Reassign enables moving pertitions between brokers using a set of commands and can output reassignment jsons for passing to the kafka cli commands",
		Args: cobra.NoArgs,
		PreRun: func(cmd *cobra.Command, args []string) {
			client = utils.NewAdminClient(brokerList, caCertFile)
		},
		PostRun: func(cmd *cobra.Command, args []string) {
			client.Close()
		},
		Run: func(cmd *cobra.Command, args []string) {
			// Build regular expression
			var topicRe *regexp.Regexp
			if len(topicFilter) > 0 {
				var err error
				topicRe, err = regexp.Compile(topicFilter)
				utils.LogAndExitIfError(logger, "Failed to compile topic filter regular expression", err)
			}
			
			reassignBrokerPartitions(logger, topicRe)
		},
	}
)

type PartitionAssignment struct {
	Topic     string   `json:"topic"`
	Partition int32    `json:"partition"`
	Replicas  []int32  `json:"replicas"`
	LogDirs   []string `json:"log_dirs,omitempty"`
}

type KafkaReassignments struct {
	Version    int                   `json:"version"`
	Partitions []PartitionAssignment `json:"partitions"`
}

func init() {
	reassignCmd.Flags().IntVarP(&fromBroker, "from", "f", -1, "Source broker id to reassign from")
	reassignCmd.Flags().IntVarP(&toBroker, "to", "t", -1, "Destination broker id to reassign to")
	reassignCmd.Flags().StringVarP(&outputJson, "output-json", "o", "Json to output reassignments to, can be fed to kafka-reassign-partitions")
	// TODO: Flag validation
	// TODO: If --to isnt set, rebalance --from broker across all remaining brokers
	// TODO: Save reassignment response config to backup original setup for undoing changes
	
	rootCmd.AddCommand(reassignCmd)
}

func reassignBrokerPartitions(topicRe *regexp.Regexp) {
	timeout, _ := time.ParseDuration("10s")
	reassignReq := sarama.AlterPartitionReassignmentsRequest{
		TimeoutMs: int32(timeout.Milliseconds()),
		Version:   int16(0), // TODO: Confluent needs 0, what does regular kafka need?
	}

	reassignments := KafkaReassignments{
		Version:    1,
		Partitions: []PartitionAssignment{},
	}

	newBlock := func(topic string, partitionId int32, replicas []int32) {
		assignment := PartitionAssignment{
			Topic:     topic,
			Partition: partitionId,
			Replicas:  replicas,
		}

		reassignments.Partitions = append(reassignments.Partitions, assignment)

		if !isDryRun {
			reassignReq.AddBlock(topic, partitionId, replicas)
		}
	}

	getPartitionReassignments(topicRe, newBlock)

	if len(outputJson) > 0 {
		bytes, err := json.MarshalIndent(reassignments, "", "  ")
		utils.LogAndExitIfError(logger, "Failed to marshall reassignment json", err)
		err = ioutil.WriteFile(outputJson, bytes, 0666)
		utils.LogAndExitIfError(logger, "Failed to write json to disk", err)
	}

	if !isDryRun {
		client.ReassignPartitions(reassignReq)

		logger.Printf("Reassignment request successful, waiting for completion")
		TrackReassignmentProgress(reassignments)
	}
}

func getPartitionReassignments(topicRe *regexp.Regexp, newBlock func(string, int32, []int32)) {
    if toBroker != -1 {
        logger.Printf("Expected reassignments after replacing broker %d with broker %d:", fromBroker, toBroker)
    } else {
        logger.Printf("Expected reassignments after rebalancing from broker %d", fromBroker)
    }

	metadata := client.GetMetadata()

	for _, topicMeta := range metadata.Topics {
		if topicRe != nil && !topicRe.MatchString(topicMeta.Name) {
			continue
		}

		for _, partitionMeta := range topicMeta.Partitions {
			// replicaMap is being used as a set
			replicaMap := make(map[int32]bool, len(partitionMeta.Replicas))
			addBlock   := false

			for _, replica := range partitionMeta.Replicas {
				if int(replica) == fromBroker {
					addBlock = true

					// TODO: If target is unset, round-robin through the existing brokers
                    if toBroker != -1 {
					    replicaMap[int32(toBroker)] = true
                	} else {	
						
                	}
				} else {
					replicaMap[replica] = true
				}
			}

			if addBlock {
				replicaSet := []int32{}
				for k := range replicaMap {
					replicaSet = append(replicaSet, k)
				}

				logger.Printf("  %s, partition: %d, replicas: %v)", topicMeta.Name, partitionMeta.ID, replicaSet)
				newBlock(topicMeta.Name, partitionMeta.ID, replicaSet)
			}
		}
	}
}
