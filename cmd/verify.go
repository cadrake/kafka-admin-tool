package cmd

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	inputJsonFile string
	
	verifyCmd = &cobra.Command{
		Use: "verify",
		Short: "Track progress of a reassignment",
		Long: "Takes an input file and checks on the clusters progress reassigning the partitions",
		Args: cobra.NoArgs,
		PreRun: func(cmd *cobra.Command, args []string) {
			client = utils.NewAdminClient(brokerList, caCertFile)
		},
		PostRun: func(cmd *cobra.Command, args []string) {
			client.Close()
		},
		Run: func(cmd *cobra.Command, args []string) {
			jsonFile, err := os.Open(inputJsonFile)
			utils.LogAndExitIfError(logger, "Failed to read reassignment json file", err)
			defer jsonFile.Close()
	
			var reassignments actions.KafkaReassignments
			err = json.NewDecoder(jsonFile).Decode(&reassignments)
			utils.LogAndExitIfError(logger, "Failed to decode json reassignment file", err)
	
			TrackReassignmentProgress(logger, reassignments)
		},
	}
)

func init() {
	verifyCmd.Flags().StringVarP(&inputJsonFile, "input-json", "i", nil, "Reassignment json to read from")
	
	rootCmd.AddCommand(verifyCmd)
}

func TrackReassignmentProgress(reassignments KafkaReassignments) {
	complete := false
	for !complete {
		timeout, _ := time.ParseDuration("10s")
		reassignListReq := sarama.ListPartitionReassignmentsRequest{
			TimeoutMs: int32(timeout.Milliseconds()),
			Version:   int16(0), // TODO: Confluent needs 0, what does regular kafka need?
		}

		for _, partitionAssignment := range reassignments.Partitions {
			reassignListReq.AddBlock(partitionAssignment.Topic, partitionAssignment.Replicas)
		}

		reassignListResp := client.ListPartitionReassignments(reassignListReq)

		if len(reassignListResp.TopicStatus) > 0 {
			logger.Printf("  Remaining:")
			for topic, idMap := range reassignListResp.TopicStatus {
				for id, status := range idMap {
					logger.Printf("    %s-%d: adding %v, removing %v", topic, id, status.AddingReplicas, status.RemovingReplicas)
				}
			}

			time.Sleep(1 * time.Second)
		} else {
			logger.Printf("Reassignment complete")
			complete = true
		}
	}
}
