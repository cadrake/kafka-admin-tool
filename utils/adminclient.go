package utils

import (
    "crypto/tls"
    "crypto/x509"
    "io/ioutil"
    "strings"

    "github.com/Shopify/sarama"
    log "github.com/sirupsen/logrus"
)

type AdminClient struct {
    client sarama.Client
}

func NewAdminClient(brokerList string, caCertFile string) *AdminClient {
    client, err := sarama.NewClient(strings.Split(brokerList, ","), getConfig(caCertFile))
    LogAndExitIfError("Failed to initialize kafka client", err)

    return &AdminClient{
        client: client,
    }
}

func (c *AdminClient) GetMetadata() *sarama.MetadataResponse {
    controller := c.getController()

    request := sarama.MetadataRequest{Topics: []string{}}
    response, err := controller.GetMetadata(&request)
    LogAndExitIfError("Failed to retrieve metadata", err)

    return response
}

func (c *AdminClient) ReassignPartitions(reassignReq sarama.AlterPartitionReassignmentsRequest) *sarama.AlterPartitionReassignmentsResponse {
    controller := c.getController()

    reassignResp, err := controller.AlterPartitionReassignments(&reassignReq)
    LogAndExitIfError("Failed to reassign partitions", err)
    LogAndExitIfKafkaError("Reassignment request failed", *reassignResp)

    return reassignResp
}

func (c *AdminClient) ListPartitionReassignments(reassignListReq sarama.ListPartitionReassignmentsRequest) *sarama.ListPartitionReassignmentsResponse {
    controller := c.getController()

    reassignListResp, err := controller.ListPartitionReassignments(&reassignListReq)
    LogAndExitIfError("Failed to retrieve active partition reassignments", err)
    LogAndExitIfKafkaError("List Reassignments failed", reassignListResp)

    return reassignListResp
}

func (c *AdminClient) Close() {
    c.client.Close()
}

func (c *AdminClient) getController() *sarama.Broker {
    controller, err := c.client.Controller()
    LogAndExitIfError("Failed to find cluster controller", err)
    return controller
}

func getConfig(caCertFile string) *sarama.Config {
    config := sarama.NewConfig()
    config.Version = sarama.V2_4_0_0

    if len(caCertFile) > 0 {
        tlsCfg, err := initTlsConfig(caCertFile)
        LogAndExitIfError("Failed to initialize TLS config", err)

        log.Info("TLS enabled")
        config.Net.TLS.Enable = true
        config.Net.TLS.Config = tlsCfg
    }

    return config
}

func initTlsConfig(caCertFile string) (*tls.Config, error) {
    if caCert, err := ioutil.ReadFile(caCertFile); err != nil {
        return nil, err
    } else {
        caCertPool := x509.NewCertPool()
        caCertPool.AppendCertsFromPEM(caCert)
        return &tls.Config{RootCAs: caCertPool}, nil
    }
}
