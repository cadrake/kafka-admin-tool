package main

import (
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/cadrake/kafka-admin-tool/cmd"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)

	cmd.Execute()
}
