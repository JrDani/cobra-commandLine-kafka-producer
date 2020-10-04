package cmd

import (
	"fmt"
	"kafka-carga/kafka"
	"os"

	"github.com/spf13/cobra"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Fazer carga de mensagens no Kafka",
	Long:  `po`,
	Run: func(cmd *cobra.Command, args []string) {
		exec()
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
}

func exec() {
	// initialize producer
	kafkaProducer, err := kafka.InitProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	// build message
	headers := make(map[string]string, 0)
	headers["header_1"] = "content_1"
	msg := "Mensagem produzida pelo Kafka Carga"

	//send message
	fmt.Println("Enviando Mensagem(s) -------------")
	kafka.Produce(msg, headers, kafkaProducer)
}
