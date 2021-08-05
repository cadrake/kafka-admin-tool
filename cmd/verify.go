package cmd

import (
    "encoding/json"
    "os"
    "time"

    "github.com/Shopify/sarama"
    log "github.com/sirupsen/logrus"
    "github.com/spf13/cobra"

    "github.com/cadrake/kafka-admin-tool/utils"
)

var (
    inputJsonFile string

    verifyCmd = &cobra.Command{
        Use: "verify",
        Short: "Track progress of a reassignment",
        Long: "Takes an input file and checks on the clusters progress reassigning the partitions",
        Args: cobra.NoArgs,
        Run: func(cmd *cobra.Command, args []string) {
            jsonFile, err := os.Open(inputJsonFile)
            utils.LogAndExitIfError("Failed to read reassignment json file", err)
            defer jsonFile.Close()

            var reassignments utils.KafkaReassignments
            err = json.NewDecoder(jsonFile).Decode(&reassignments)
            utils.LogAndExitIfError("Failed to decode json reassignment file", err)

            TrackReassignmentProgress(&reassignments)
        },
    }
)

func init() {
    verifyCmd.Flags().StringVarP(&inputJsonFile, "input-json", "i", "", "Reassignment json to read from")

    rootCmd.AddCommand(verifyCmd)
}

func TrackReassignmentProgress(reassignments *utils.KafkaReassignments) {
    complete := false
    for !complete {
        timeout, _ := time.ParseDuration("10s")
        reassignListReq := sarama.ListPartitionReassignmentsRequest{
            TimeoutMs: int32(timeout.Milliseconds()),
            Version:   int16(0),
        }

        for _, partitionAssignment := range reassignments.Partitions {
            reassignListReq.AddBlock(partitionAssignment.Topic, partitionAssignment.Replicas)
        }

        reassignListResp := client.ListPartitionReassignments(reassignListReq)

        if len(reassignListResp.TopicStatus) > 0 {
            log.Infof("  Remaining:")
            for topic, idMap := range reassignListResp.TopicStatus {
                for id, status := range idMap {
                    log.Infof("    %s-%d: adding %v, removing %v", topic, id, status.AddingReplicas, status.RemovingReplicas)
                }
            }

            time.Sleep(1 * time.Second)
        } else {
            log.Infof("Reassignment complete")
            complete = true
        }
    }
}
