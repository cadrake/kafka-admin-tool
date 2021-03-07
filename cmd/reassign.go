package cmd

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "time"

    "github.com/Shopify/sarama"
    "github.com/spf13/cobra"

    "cadrake/kafka-admin-tool/utils"
)

var (
    fromBroker int32
    toBroker int32
    outputJson string
    
    reassignCmd = &cobra.Command{
        Use: "reassign",
        Short: "Reassign partitions in a cluster",
        Long: "Reassign enables moving pertitions between brokers using a set of commands and can output reassignment jsons for passing to the kafka cli commands",
        Args: cobra.NoArgs,
        PreRunE: func(cmd *cobra.Command, args []string) error {
            if fromBroker == -1 {
                return fmt.Errorf("Missing required flag --from")
            }
            if fromBroker == toBroker {
                return fmt.Errorf("From broker (%d) is the same as to broker (%d)", fromBroker, toBroker)
            }
            return nil
        },
        Run: func(cmd *cobra.Command, args []string) {            
            reassignBrokerPartitions()
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
    // TODO: Option to select partitions to move
    reassignCmd.Flags().Int32VarP(&fromBroker, "from", "f", -1, "Source broker id to reassign from")
    reassignCmd.Flags().Int32VarP(&toBroker, "to", "t", -1, "Destination broker id to reassign to")
    reassignCmd.Flags().StringVarP(&outputJson, "output-json", "o", "", "Json to output reassignments to, can be fed to kafka-reassign-partitions")
    
    rootCmd.AddCommand(reassignCmd)
}

func reassignBrokerPartitions() {
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

    getPartitionReassignments(newBlock)

    if len(outputJson) > 0 {
        bytes, err := json.MarshalIndent(reassignments, "", "  ")
        utils.LogAndExitIfError(logger, "Failed to marshall reassignment json", err)
        err = ioutil.WriteFile(outputJson, bytes, 0666)
        utils.LogAndExitIfError(logger, "Failed to write json to disk", err)
    }

    if !isDryRun {
        logger.Printf("Sending reassignments to cluster")
        client.ReassignPartitions(reassignReq)

        logger.Printf("Reassignment request successful, waiting for completion")
        TrackReassignmentProgress(reassignments)
    } else {
        logger.Printf("Run again with the --execute flag to apply the changes")
    }
}

func getPartitionReassignments(newBlock func(string, int32, []int32)) {
    if toBroker != -1 {
        logger.Printf("Expected reassignments after replacing broker %d with broker %d:", fromBroker, toBroker)
    } else {
        logger.Printf("Expected reassignments after rebalancing from broker %d", fromBroker)
    }

    metadata := client.GetMetadata()
    safeBrokers := []int32{}
    for _, broker := range metadata.Brokers {
        if broker.ID() != fromBroker {
            safeBrokers = append(safeBrokers, broker.ID())
        }
    }

    // TODO: Refactor this to be more time efficient if possible
    for _, topicMeta := range metadata.Topics {
        if !topicRe.MatchString(topicMeta.Name) {
            continue
        }

        nextBroker := 0
        for _, partitionMeta := range topicMeta.Partitions {
            // replicaMap is being used as a set
            replicaMap    := make(map[int32]bool, len(partitionMeta.Replicas))
            addBlock      := false
            needNewBroker := false

            for _, replica := range partitionMeta.Replicas {
                if replica == fromBroker {
                    addBlock = true

                    if toBroker != -1 {
                        // Replace broker in map
                        replicaMap[int32(toBroker)] = true
                    } else {
                        // Need to find an unassigned broker for this replica
                        needNewBroker = true
                    }
                } else {
                    replicaMap[replica] = true
                }
            }

            // Attempt to find a new home for this replica by rotating through remaining brokers until finding an open one
            if needNewBroker {
                for i := 0; i < len(safeBrokers); i++ {
                    broker := safeBrokers[nextBroker % len(safeBrokers)]
                    nextBroker++

                    if _, exists := replicaMap[broker]; !exists {
                        replicaMap[broker] = true
                        break;
                    }
                }
            }

            if addBlock {
                replicaSet := []int32{}
                for k := range replicaMap {
                    replicaSet = append(replicaSet, k)
                }

                logger.Printf("  %s, partition: %d, replicas: %v -> %v)", topicMeta.Name, partitionMeta.ID, partitionMeta.Replicas, replicaSet)
                newBlock(topicMeta.Name, partitionMeta.ID, replicaSet)
            }
        }
    }
}
