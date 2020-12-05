package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type KafkaResponseError struct {
	ErrorCode    sarama.KError
	ErrorMessage *string
}

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

func main() {
	logger := log.New(os.Stdout, "[kafka-admin-tool] ", log.LstdFlags)

	var brokerList, topicFilter, caCert, inputJson, outputJson string
	var sourceBroker, targetBroker, replicationFactor, replicationDelta int
	var isReassign, isVerify, isAlterTopic, doExecute bool

	// General Options
	flag.StringVar(&brokerList, "broker-list", "localhost:9092", "Kafka brokers to connect to")
	flag.StringVar(&topicFilter, "topic-filter", "", "Regular expression used to match topics to reassign")
	flag.StringVar(&caCert, "ca-cert", "", "Location of ca certificate for ssl communication with cluster")
	flag.BoolVar(&doExecute, "execute", false, "True to execute the admin command")

	// TODO: If --to isnt set, rebalance --from broker across all remaining brokers
	// TODO: Save reassignment response config to persist original setup for undoing changes
	// Partition Reassignment Commands
	flag.BoolVar(&isReassign, "reassign", false, "Enable reassign partitions mode")
	flag.IntVar(&sourceBroker, "from", -1, "Source broker to move partitions from")
	flag.IntVar(&targetBroker, "to", -1, "Target broker to move partitions to")
	flag.StringVar(&outputJson, "output-json", "", "Print json in format that can be handed to kafka-reassign-partitions and save to file")

	// Reassignment Verification Commands
	flag.BoolVar(&isVerify, "verify", false, "Monitor progress of a given reassignment json file")
	flag.StringVar(&inputJson, "input-json", "", "Reassignment json file to track progress of")

	// Topic Alteration Commands
	flag.BoolVar(&isAlterTopic, "alter-topic", false, "Enable alter topic mode")
	flag.IntVar(&replicationFactor, "set-replication", -1, "New replication factor to set on topic")
	flag.IntVar(&replicationDelta, "delta-replication", 0, "Delta to adjust replication factor by")

	flag.Parse()

	// Check all bools and barf if multiple true
	checkFlag := func(count *int, flag bool) {
		if flag {
			*count++
		}
	}

	commandOptCount := 0
	checkFlag(&commandOptCount, isReassign)
	checkFlag(&commandOptCount, isVerify)
	checkFlag(&commandOptCount, isAlterTopic)
	if commandOptCount != 1 {
		logger.Printf("Error: incorrect number of commands selected, one of --reassign, --verify or --alter-topic is required")
		os.Exit(1)
	}

	// Build regular expression
	var topicRe *regexp.Regexp
	if len(topicFilter) > 0 {
		var err error
		topicRe, err = regexp.Compile(topicFilter)
		logAndExitIfError(logger, "Failed to compile topic filter regular expression", err)
	}

	logger.Printf("Connecting to brokers: %s", brokerList)

	client, err := sarama.NewClient(strings.Split(brokerList, ","), getConfig(logger, caCert))
	logAndExitIfError(logger, "Failed to initialize kafka client", err)

	controller, err := client.Controller()
	logAndExitIfError(logger, "Failed to find cluster controller", err)

	request := sarama.MetadataRequest{Topics: []string{}}
	response, err := controller.GetMetadata(&request)
	logAndExitIfError(logger, "Failed to retrieve metadata", err)

	// TODO: Take rack awareness into account when generating reassignments
	// TODO: Figure out way to emulate --generate-json

	// TODO: Retest all commands
	if isReassign {
		reassignBrokerPartitions(logger, response, controller, topicRe, sourceBroker, targetBroker, doExecute, outputJson)
	} else if isAlterTopic {
		// TODO Validate either replicationDelta or replicationFactor set but not both
		// TODO: Will need to reassign new partitions after an increase in RF
		alterTopics(logger, response, controller, topicRe, doExecute, replicationDelta, replicationFactor)
	} else if isVerify {
		bytes, err := ioutil.ReadFile(inputJson)
		logAndExitIfError(logger, "Failed to read reassignment json file", err)

		var reassignments KafkaReassignments
		json.Unmarshal(bytes, &reassignments)
		trackReassignmentProgress(logger, *controller, reassignments)
	}

	client.Close()
}

func reassignBrokerPartitions(logger *log.Logger, metadata *sarama.MetadataResponse, controller *sarama.Broker, topicRe *regexp.Regexp, sourceBroker int, targetBroker int, doExecute bool, outputJson string) {
	timeout, _ := time.ParseDuration("10s")
	reassignReq := sarama.AlterPartitionReassignmentsRequest{
		TimeoutMs: int32(timeout.Milliseconds()),
		Version:   int16(1),
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

		if doExecute {
			reassignReq.AddBlock(topic, partitionId, replicas)
		}
	}

	getPartitionReassignments(logger, metadata, topicRe, sourceBroker, targetBroker, newBlock)

	if len(outputJson) > 0 {
		bytes, err := json.MarshalIndent(reassignments, "", "  ")
		logAndExitIfError(logger, "Failed to marshall reassignment json", err)
		err = ioutil.WriteFile(outputJson, bytes, 0666)
		logAndExitIfError(logger, "Failed to write json to disk", err)
	}

	if doExecute {
		applyPartitionReassignments(logger, *controller, reassignReq)

		logger.Printf("Reassignment request successful, waiting for completion")
		trackReassignmentProgress(logger, *controller, reassignments)
	}
}

func alterTopics(logger *log.Logger, metadata *sarama.MetadataResponse, controller *sarama.Broker, topicRe *regexp.Regexp, doExecute bool, replicationDelta int, replicationFactor int) {
	request := buildTopicAlterConfig(logger, metadata, topicRe, doExecute, replicationDelta, replicationFactor)
	response, err := controller.AlterConfigs(&request)
	logAndExitIfError(logger, "Failed to alter topic configuration", err)
}

func applyPartitionReassignments(logger *log.Logger, controller sarama.Broker, reassignReq sarama.AlterPartitionReassignmentsRequest) {
	reassignResp, err := controller.AlterPartitionReassignments(&reassignReq)
	logAndExitIfError(logger, "Failed to reassign partitions", err)
	logAndExitIfKafkaError(logger, "Reassignment request failed", *reassignResp)
}

func trackReassignmentProgress(logger *log.Logger, controller *sarama.Broker, reassignments KafkaReassignments) {
	complete := false
	for !complete {
		timeout, _ := time.ParseDuration("10s")
		reassignListReq := sarama.ListPartitionReassignmentsRequest{
			TimeoutMs: int32(timeout.Milliseconds()),
			Version:   int16(1),
		}

		for _, partitionAssignment := range reassignments.Partitions {
			reassignListReq.AddBlock(partitionAssignment.Topic, partitionAssignment.Replicas)
		}

		reassignListResp, err := controller.ListPartitionReassignments(&reassignListReq)
		logAndExitIfError(logger, "Failed to retrieve active partition reassignments", err)
		logAndExitIfKafkaError(logger, "List Reassignments failed", reassignListResp)

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

func buildTopicAlterConfig(logger *log.Logger, metadata *sarama.MetadataResponse, topicRe *regexp.Regexp, doExecute bool, replicationDelta int, replicationFactor int) sarama.AlterConfigsRequest {
	request := sarama.AlterConfigsRequest{
		ValidateOnly: !doExecute,
		Resources:    []*sarama.AlterConfigsResource{},
	}

	for _, topicMeta := range metadata.Topics {
		if topicRe != nil && !topicRe.MatchString(topicMeta.Name) {
			continue
		}

		var newFactor string
		if replicationDelta != 0 {
			// TODO: Get existing RF and apply delta
		} else {
			newFactor = strconv.Itoa(replicationFactor)
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

func getPartitionReassignments(logger *log.Logger, metadata *sarama.MetadataResponse, topicRe *regexp.Regexp, sourceBroker int, targetBroker int, newBlock func(string, int32, []int32)) {
	logger.Printf("Expected reassignments after replacing broker %d with broker %d:", sourceBroker, targetBroker)

	for _, topicMeta := range metadata.Topics {
		if topicRe != nil && !topicRe.MatchString(topicMeta.Name) {
			continue
		}

		for _, partitionMeta := range topicMeta.Partitions {
			replicaMap := make(map[int32]bool, len(partitionMeta.Replicas))
			addBlock := false

			for _, replica := range partitionMeta.Replicas {
				if int(replica) == sourceBroker {
					addBlock = true
					replicaMap[int32(targetBroker)] = true
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

// TODO: Set version based on admin action
func getConfig(logger *log.Logger, caCert string) *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_4_0_0 // AlterPartitionReassignmentsRequest: V2_4_0_0, AlterConfigsRequest: V1_0_0_0

	if len(caCert) > 0 {
		tlsCfg, err := initTlsConfig(caCert)
		logAndExitIfError(logger, "Failed to initialize TLS config", err)

		logger.Print("TLS enabled")
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsCfg
	}

	return config
}

func logAndExitIfError(logger *log.Logger, reason string, err error) {
	if err != nil {
		logger.Printf("%s, %v", reason, err)
		os.Exit(1)
	}
}

// Expects a sarama response that contains ErrorCode and ErrorMessage
func logAndExitIfKafkaError(logger *log.Logger, reason string, error interface{}) {
	respError, _ := error.(KafkaResponseError)
	if respError.ErrorCode != 0 {
		logger.Printf("%s: Code: %d, Message: %s", reason, respError.ErrorCode, respError.ErrorMessage)
	}
}

func initTlsConfig(certFile string) (*tls.Config, error) {
	if caCert, err := ioutil.ReadFile(certFile); err != nil {
		return nil, err
	} else {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		return &tls.Config{RootCAs: caCertPool}, nil
	}
}
