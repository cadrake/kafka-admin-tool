package cmd

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "time"

    "github.com/Shopify/sarama"
    log "github.com/sirupsen/logrus"
    "github.com/spf13/cobra"

    "github.com/cadrake/kafka-admin-tool/utils"
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

func init() {
    reassignCmd.Flags().Int32VarP(&fromBroker, "from", "f", -1, "Source broker id to reassign from")
    reassignCmd.Flags().Int32VarP(&toBroker, "to", "t", -1, "Destination broker id to reassign to")
    reassignCmd.Flags().StringVarP(&outputJson, "output-json", "o", "", "Json to output reassignments to, can be fed to kafka-reassign-partitions")

    rootCmd.AddCommand(reassignCmd)
}

func reassignBrokerPartitions() {
    reassignments := &utils.KafkaReassignments{
        Version:    1,
        Partitions: []utils.PartitionAssignment{},
    }

    reassignReq := getPartitionReassignments(reassignments)

    if len(outputJson) > 0 {
        bytes, err := json.MarshalIndent(reassignments, "", "  ")
        utils.LogAndExitIfError("Failed to marshall reassignment json", err)
        err = ioutil.WriteFile(outputJson, bytes, 0666)
        utils.LogAndExitIfError("Failed to write json to disk", err)
        log.Infof("Reassignments saved to '%s'", outputJson)
    }

    if doExecute {
        log.Infof("Sending reassignments to cluster")
        client.ReassignPartitions(reassignReq)

        log.Infof("Reassignment request successful, waiting for completion")
        TrackReassignmentProgress(reassignments)
    } else {
        log.Infof("Run again with the --execute flag to apply the changes")
    }
}

func getPartitionReassignments(reassignments *utils.KafkaReassignments) sarama.AlterPartitionReassignmentsRequest {
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

    if toBroker != -1 {
        log.Infof("Expected reassignments after replacing broker %d with broker %d:", fromBroker, toBroker)
    } else {
        log.Infof("Expected reassignments after rebalancing from broker %d", fromBroker)
    }

    for _, topicMeta := range metadata.Topics {
        if !topicRe.MatchString(topicMeta.Name) {
            continue
        }

        config := utils.RebalanceConfig{
            RequiredBroker: toBroker,
            TaintedBroker: fromBroker,
            FinalReplicaCount: len(topicMeta.Partitions[0].Replicas),
        }
        utils.RebalanceTopicBrokers(topicMeta, metadata.Brokers, config, newBlock)
    }

    return reassignReq
}
