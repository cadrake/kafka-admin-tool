package utils

import (
    "crypto/tls"
    "crypto/x509"
    "io/ioutil"
    "log"
    "os"
    "strings"

    "github.com/Shopify/sarama"
)

type AdminClient struct {
    client sarama.Client
    logger *log.Logger
}

func NewAdminClient(brokerList string, caCertFile string) *AdminClient {
    logger := log.New(os.Stdout, "[kafka-admin-client]", log.LstdFlags)
    client, err := sarama.NewClient(strings.Split(brokerList, ","), getConfig(logger, caCertFile))
    LogAndExitIfError(logger, "Failed to initialize kafka client", err)
    
    return &AdminClient{
        client: client,
        logger: logger,
    }
}

func (c *AdminClient) GetMetadata() *sarama.MetadataResponse {
    controller := c.getController()

    request := sarama.MetadataRequest{Topics: []string{}}
    response, err := controller.GetMetadata(&request)
    LogAndExitIfError(c.logger, "Failed to retrieve metadata", err)

    return response
}

func (c *AdminClient) ReassignPartitions(reassignReq sarama.AlterPartitionReassignmentsRequest) *sarama.AlterPartitionReassignmentsResponse {
    controller := c.getController()
    
    reassignResp, err := controller.AlterPartitionReassignments(&reassignReq)
    LogAndExitIfError(c.logger, "Failed to reassign partitions", err)
    LogAndExitIfKafkaError(c.logger, "Reassignment request failed", *reassignResp)

    return reassignResp
}

func (c *AdminClient) ListPartitionReassignments(reassignListReq sarama.ListPartitionReassignmentsRequest) *sarama.ListPartitionReassignmentsResponse {
    controller := c.getController()

    reassignListResp, err := controller.ListPartitionReassignments(&reassignListReq)
    LogAndExitIfError(c.logger, "Failed to retrieve active partition reassignments", err)
    LogAndExitIfKafkaError(c.logger, "List Reassignments failed", reassignListResp)

    return reassignListResp
}

func (c *AdminClient) AlterConfigs(alterReq sarama.AlterConfigsRequest) *sarama.AlterConfigsResponse{
    controller := c.getController()

    response, err := controller.AlterConfigs(&alterReq)
    LogAndExitIfError(c.logger, "Failed to alter topic configuration", err)

    return response
}

func (c *AdminClient) Close() {
    c.client.Close()
}

func (c *AdminClient) getController() *sarama.Broker {
    controller, err := c.client.Controller()
    LogAndExitIfError(c.logger, "Failed to find cluster controller", err)
    return controller
}

// TODO: Set version based on admin action
func getConfig(logger *log.Logger, caCertFile string) *sarama.Config {
    config := sarama.NewConfig()
    config.Version = sarama.V2_5_0_0 // AlterPartitionReassignmentsRequest: V2_4_0_0, AlterConfigsRequest: V1_0_0_0

    if len(caCertFile) > 0 {
        tlsCfg, err := initTlsConfig(caCertFile)
        LogAndExitIfError(logger, "Failed to initialize TLS config", err)

        logger.Print("TLS enabled")
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
