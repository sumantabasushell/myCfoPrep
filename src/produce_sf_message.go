package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config map to hold credential values from config file.
type Config map[string]string

// wrote a custom function to read credential details from properties file and store in local variables

func ReadConfig(filename string) (Config, error) {
	// init with some bogus data
	config := Config{

		"redpandausername": "*****",
		"redpandapassword": "*****",
	}

	if len(filename) == 0 {
		return config, nil
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadString('\n')

		// check if the line has = sign
		// and process the line. Ignore the rest.
		if equal := strings.Index(line, "="); equal >= 0 {
			if key := strings.TrimSpace(line[:equal]); len(key) > 0 {
				value := ""
				if len(line) > equal {
					value = strings.TrimSpace(line[equal+1:])
				}
				// assign the config map
				config[key] = value
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return config, nil
}

func main() {
	topic := "test-cfo-002"
	hostname, _ := os.Hostname()
	ctx := context.Background()

	// capture message to be sent to RedPanda as argument
	args := os.Args[1]
	// RedPanda broker details with port
	seeds := []string{"seed-389b8673.chv1fce8mbdudchrjttg.fmc.prd.cloud.redpanda.com:9092"}
	opts := []kgo.Opt{}
	opts = append(opts,
		kgo.SeedBrokers(seeds...),
	)

	// Initialize public CAs for TLS
	opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))

	// Retrieve username and password from config file
	config, err := ReadConfig(`/Users/sumanta.basu/MyRnD/golang/Salesforce/Salesforce.properties`)

	// Handle error
	if err != nil {
		fmt.Println(err)
	}

	// Initializes SASL/SCRAM 256
	opts = append(opts, kgo.SASL(scram.Auth{
		User: config["redpandausername"],
		Pass: config["redpandapassword"],
	}.AsSha256Mechanism()))

	// Initializes SASL/SCRAM 512
	/*
		  opts = append(opts, kgo.SASL(scram.Auth{
				User: "Producer123",
				Pass: "<password>",
			}.AsSha512Mechanism()))
	*/

	client, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Produce message asynchronously
	var wg sync.WaitGroup
	//for i := 1; i < 5; i++ {
	wg.Add(1)
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(hostname),

		Value: []byte(fmt.Sprintf("{\"Description\":\"%s\"}", args)),
	}
	client.Produce(ctx, record, func(record *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("Error sending message: %v \n", err)
		} else {
			fmt.Printf("Message sent: topic: %s, offset: %d, value: %s \n",
				topic, record.Offset, record.Value)
		}
	})
	//}
	wg.Wait()

}
