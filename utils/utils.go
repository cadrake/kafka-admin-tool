package utils

import (
	"github.com/Shopify/sarama"
    log "github.com/sirupsen/logrus"
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

type RebalanceConfig struct {
    RequiredBroker int32
    TaintedBroker int32
    FinalReplicaCount int
}

func LogAndExitIfError(reason string, err error) {
    if err != nil {
        log.Fatalf("%s, %v", reason, err)
    }
}

// Deserializes a sarama response that contains ErrorCode and ErrorMessage
func LogAndExitIfKafkaError(reason string, error interface{}) {
    respError, _ := error.(KafkaResponseError)
    if respError.ErrorCode != 0 {
        log.Fatalf("%s: Code: %d, Message: %s", reason, respError.ErrorCode, *respError.ErrorMessage)
    }
}

// Take a topic and rebalance the brokers in each partition, will attempt to include required broker and will always
// remove tainted broker while ensuring a specified number of replicas remain, calls commitFn once per partition if the
// replica set has changed
func RebalanceTopicBrokers(topicMeta *sarama.TopicMetadata, brokerList []*sarama.Broker, rebalanceConfig RebalanceConfig, commitFn func(string, int32, []int32)) {
    brokerIndex := -1
    for _, partitionMeta := range topicMeta.Partitions {
        updatedReplicasSet := make(map[int32]bool, len(partitionMeta.Replicas))

        // First get all replicas we are going to keep
        for _, replica := range partitionMeta.Replicas {
            if replica != rebalanceConfig.TaintedBroker {
                updatedReplicasSet[replica] = true
            }
        }

        // Now add any missing required replicas if there is room
        if (rebalanceConfig.RequiredBroker != -1) && (len(updatedReplicasSet) < rebalanceConfig.FinalReplicaCount) {
            updatedReplicasSet[rebalanceConfig.RequiredBroker] = true
        }

        // If the desired replication factor is equal to the number of brokers and we have a valid rebalanceConfig.TaintedBroker, commit and continue
        if (len(brokerList) == rebalanceConfig.FinalReplicaCount) && (rebalanceConfig.TaintedBroker != -1) {
            commitChanges(topicMeta.Name, partitionMeta.ID, partitionMeta.Replicas, updatedReplicasSet, commitFn)
            continue
        }

        // Adjust replica set to match rebalanceConfig.FinalReplicaCount
        for len(updatedReplicasSet) < rebalanceConfig.FinalReplicaCount {
            brokerIndex++
            brokerId := brokerList[brokerIndex % len(brokerList)].ID()

            for !isValidNewBroker(brokerId, rebalanceConfig.TaintedBroker, updatedReplicasSet, brokerList) {
                brokerIndex++
                brokerId = brokerList[brokerIndex % len(brokerList)].ID()
            }

            updatedReplicasSet[brokerId] = true
        }

        for len(updatedReplicasSet) > rebalanceConfig.FinalReplicaCount {
            brokerIndex++
            brokerId := brokerList[brokerIndex % len(brokerList)].ID()

            for !isValidDeleteBroker(brokerId, rebalanceConfig.TaintedBroker, updatedReplicasSet, brokerList) {
                brokerIndex++
                brokerId = brokerList[brokerIndex % len(brokerList)].ID()
            }

            delete(updatedReplicasSet, brokerId)
        }

        commitChanges(topicMeta.Name, partitionMeta.ID, partitionMeta.Replicas, updatedReplicasSet, commitFn)
    }
}

func commitChanges(topicName string, partitionId int32, oldReplicas []int32, newReplicaMap map[int32]bool, commitFn func(string, int32, []int32)) {
    newReplicas := []int32{}
    for k := range newReplicaMap {
        newReplicas = append(newReplicas, k)
    }

    log.Infof("  %s, partition: %d, replicas: %v -> %v)", topicName, partitionId, oldReplicas, newReplicas)
    if !arrayEqualsIgnoreOrder(oldReplicas, newReplicas) {
        commitFn(topicName, partitionId, newReplicas)
    }
}

func arrayEqualsIgnoreOrder(array1 []int32, array2 []int32) bool {
    if len(array1) != len(array2) {
        return false
    }

    countMap := make(map[int32]int, len(array1))
    for _, val := range array1 {
        if _, exists := countMap[val]; !exists {
            countMap[val] = 0
        }
        countMap[val]++
    }

    for _, val := range array2 {
        if _, exists := countMap[val]; !exists {
            return false
        }

        countMap[val]--
        if countMap[val] == 0 {
            delete(countMap, val)
        }
    }

    return len(countMap) == 0
}

func isValidNewBroker(brokerId int32, taintedBroker int32, existingBrokers map[int32]bool, brokerList []*sarama.Broker) bool {
    if _, exists := existingBrokers[brokerId]; exists {
        return false
    }

    return brokerId != taintedBroker
}

func isValidDeleteBroker(brokerId int32, taintedBroker int32, existingBrokers map[int32]bool, brokerList []*sarama.Broker) bool {
    if _, exists := existingBrokers[brokerId]; !exists {
        return false
    }
    return true
}
