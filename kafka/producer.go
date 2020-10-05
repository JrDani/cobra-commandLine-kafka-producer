package kafka

import (
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

// InitProducer ...
func InitProducer() (sarama.SyncProducer, error) {
	var host = viper.GetString("kafka.host")
	var port = viper.GetString("kafka.port")
	var conn = host + ":" + port

	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	// producer config
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.V0_11_0_0

	// create producer
	prd, err := sarama.NewSyncProducer([]string{conn}, config)

	return prd, err
}

// Produce ...
func Produce(message string, headers map[string]string, producer sarama.SyncProducer) {
	var topic = viper.GetString("kafka.topic")

	// publish sync
	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.StringEncoder(message),
		Headers: convertHeaders(headers),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}

	fmt.Println("Partition: ", p)
	fmt.Println("Offset: ", o)
	fmt.Println("Done!")
}

func convertHeaders(headers map[string]string) []sarama.RecordHeader {
	output := make([]sarama.RecordHeader, 0)
	for key, value := range headers {
		output = append(output, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}
	return output
}
