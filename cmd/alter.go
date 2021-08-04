package cmd

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
    log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

    "cadrake/kafka-admin-tool/utils"
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
            alterTopicReplicationFactor()
        },
    }
)

func init() {
    alterCmd.Flags().IntVar(&newReplicationFactor, "new-rf", -1, "New replication factor value for topics")
    alterCmd.Flags().IntVar(&deltaReplicationFactor, "delta-rf", 0, "Amount to increase/decrease replication factor of topics")

    rootCmd.AddCommand(alterCmd)
}

func alterTopicReplicationFactor() {
    reassignments := &utils.KafkaReassignments{
        Version:    1,
        Partitions: []utils.PartitionAssignment{},
    }

    reassignReq := buildTopicAlterConfig(reassignments)
    if doExecute {
        log.Infof("Sending reassignments to cluster")
        client.ReassignPartitions(reassignReq)

        log.Infof("Reassignment request successful, waiting for completion")
        TrackReassignmentProgress(reassignments)
    } else {
        log.Infof("Run again with the --execute flag to apply the changes")
    }
}

func buildTopicAlterConfig(reassignments *utils.KafkaReassignments) sarama.AlterPartitionReassignmentsRequest {
    timeout, _ := time.ParseDuration("10s")
    metadata := client.GetMetadata()
    reassignReq := sarama.AlterPartitionReassignmentsRequest{
        TimeoutMs: int32(timeout.Milliseconds()),
        Version:   int16(0),
    }

    newBlock := func(topic string, partitionId int32, replicas []int32) {
        assignment := utils.PartitionAssignment{
            Topic:     topic,
            Partition: partitionId,
            Replicas:  replicas,
        }

        reassignments.Partitions = append(reassignments.Partitions, assignment)

        if doExecute {
            reassignReq.AddBlock(topic, partitionId, replicas)
        }
    }

    if deltaReplicationFactor != 0 {
        log.Infof("Expected reassignments after altering replication factor by %d", deltaReplicationFactor)
    } else {
        log.Infof("Expected reassignments after setting replication factor to %d", newReplicationFactor)
    }

    for _, topicMeta := range metadata.Topics {
        if topicRe != nil && !topicRe.MatchString(topicMeta.Name) {
            continue
        }

        var newFactor int
        if deltaReplicationFactor != 0 {
            newFactor = len(topicMeta.Partitions[0].Replicas) + deltaReplicationFactor
        } else {
            newFactor = newReplicationFactor
        }

        if (newFactor < 1) || (newFactor > len(metadata.Brokers)) {
            log.Warnf("New replication factor %d for topic %s is invalid", newFactor, topicMeta.Name)
            continue
        }

        // Build rebalance config and reassignment request
        config := utils.RebalanceConfig{
            RequiredBroker: -1,
            TaintedBroker: -1,
            FinalReplicaCount: newFactor,
        }
        utils.RebalanceTopicBrokers(topicMeta, metadata.Brokers, config, newBlock)
    }

    return reassignReq
}
