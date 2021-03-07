package utils

import (
    "log"
    "os"

	"github.com/Shopify/sarama"
)

type KafkaResponseError struct {
	ErrorCode    sarama.KError
	ErrorMessage *string
}

func LogAndExitIfError(logger *log.Logger, reason string, err error) {
	if err != nil {
		logger.Printf("%s, %v", reason, err)
		os.Exit(1)
	}
}

// Deserializes a sarama response that contains ErrorCode and ErrorMessage
func LogAndExitIfKafkaError(logger *log.Logger, reason string, error interface{}) {
	respError, _ := error.(KafkaResponseError)
	if respError.ErrorCode != 0 {
		logger.Printf("%s: Code: %d, Message: %s", reason, respError.ErrorCode, respError.ErrorMessage)
	}
}
