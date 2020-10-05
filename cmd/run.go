package cmd

import (
	"fmt"
	"io/ioutil"
	"kafka-carga/kafka"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "load Kafka messages",
	Long:  `load Kafka messages using files content`,
	Run: func(cmd *cobra.Command, args []string) {
		exec()
	},
}

var repeat int
var targetPath string

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().IntVarP(&repeat, "repeat", "r", 1, "Number of times that each message will be send")
	runCmd.Flags().StringVarP(&targetPath, "target", "t", "", "The path of the file or directory")
}

func exec() {
	// initialize producer
	kafkaProducer, err := kafka.InitProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	// build messages
	messages := buildMessage()
	headers := make(map[string]string, 0)
	headers["header_1"] = "content_1"

	// send message
	fmt.Printf("Sending %d Message(s) -------------\n", len(messages))
	for _, msg := range messages {
		kafka.Produce(msg, headers, kafkaProducer)
	}
}

func buildMessage() []string {
	if isFile() {
		content := readFileContent(targetPath)
		return []string{string(content)}
	}

	allFiles := getAllFilesInDirectory()
	filesContent := readAllFiles(allFiles)
	return filesContent
}

func isFile() bool {
	return filepath.Ext(targetPath) != ""
}

func getAllFilesInDirectory() []os.FileInfo {
	files, err := ioutil.ReadDir(targetPath)
	if err != nil {
		log.Fatal(err)
	}
	return files
}

func readAllFiles(files []os.FileInfo) []string {
	var filesContent = make([]string, len(files))
	for _, file := range files {
		content := readFileContent(targetPath + "/" + file.Name())
		filesContent = append(filesContent, string(content))
	}
	return filesContent
}

func readFileContent(fileName string) []byte {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Println(err)
	}
	return content
}
