package cmd

import (
	"fmt"
	"io/ioutil"
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

var repeat int
var targetPath string

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().IntVarP(&repeat, "repeat", "r", 1, "Quantidade de vezes que cada mensagem será enviada.")
	runCmd.Flags().StringVarP(&targetPath, "target", "t", "", "Caminho do arquivo ou do diretório a ser transformado em mensagem.")
}

func exec() {
	// initialize producer
	kafkaProducer, err := kafka.InitProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	msg := buildMessage()
	headers := make(map[string]string, 0)
	headers["header_1"] = "content_1"

	//send message
	fmt.Println("Enviando Mensagem(s) -------------")
	kafka.Produce(msg, headers, kafkaProducer)
}

func buildMessage() string {
	fileContent, err := ioutil.ReadFile(targetPath)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Enviando o texto: ", string(fileContent))

	return string(fileContent)
}
