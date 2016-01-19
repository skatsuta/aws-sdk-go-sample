package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var (
	region   = flag.String("region", "us-east-1", "AWS region")
	queueURL = flag.String("queue-url", "", "SQS URL. Required")
	delete   = flag.Bool("delete", false, "delete received messages")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %v -queue-url <queue-url> [-region <region>] [-delete] [mesages...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// queue url is required
	if *queueURL == "" {
		fmt.Fprintln(os.Stderr, "-queue-url is required")
		os.Exit(1)
	}

	// create an SQS client object
	svc := sqs.New(session.New(&aws.Config{Region: region}))

	msgs := flag.Args()
	if len(msgs) > 0 {
		if err := sendMessage(svc, msgs...); err != nil {
			log.Fatal(err)
		}
	}

	count := int64(10)
	rmsgs, err := receiveMessage(svc, count)
	if err != nil {
		log.Fatal(err)
	}

	if *delete {
		if err := deleteMessage(svc, rmsgs...); err != nil {
			log.Fatal(err)
		}
	}
}

// sendMessage sends msgs to SQS.
func sendMessage(svc *sqs.SQS, msgs ...string) error {
	// create message send params
	sendParams := sqs.SendMessageInput{
		QueueUrl: queueURL,
	}

	// send message
	for i, msg := range msgs {
		// message body is a datetime string
		sendParams.MessageBody = aws.String(msg)
		_, err := svc.SendMessage(&sendParams)
		if err != nil {
			return err
		}

		fmt.Printf("message sent %v: %v\n", i, msg)
	}

	return nil
}

// receiveMessage receives messages up to count.
func receiveMessage(svc *sqs.SQS, count int64) ([]*sqs.Message, error) {
	// create message receiption params
	rcvParams := sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: aws.Int64(count),
	}

	// receive message
	res, err := svc.ReceiveMessage(&rcvParams)
	if err != nil {
		return nil, err
	}

	for i, msg := range res.Messages {
		fmt.Printf("message received %v: %v\n", i, *msg.Body)
	}

	return res.Messages, nil
}

func deleteMessage(svc *sqs.SQS, msgs ...*sqs.Message) error {
	// delete messages
	delParams := sqs.DeleteMessageInput{
		QueueUrl: queueURL,
	}

	for i, msg := range msgs {
		delParams.ReceiptHandle = msg.ReceiptHandle
		_, err := svc.DeleteMessage(&delParams)
		if err != nil {
			return err
		}

		fmt.Printf("message deleted %v: %v\n", i, *msg.Body)
	}

	return nil
}
