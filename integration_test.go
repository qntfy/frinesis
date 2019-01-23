package frinesis_test

import (
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	k "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/qntfy/frinesis"
	"github.com/qntfy/frizzle"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

const (
	defaultKinesisEndpoint = "127.0.0.1:4568"
)

type sinkTestSuite struct {
	suite.Suite
	kin   *k.Kinesis
	sink  frizzle.Sink
	topic string
	shard string
}

func loadKinesisTestENV() *viper.Viper {
	// Setup viper and config from ENV
	v := viper.New()
	v.AutomaticEnv()
	v.SetDefault("kinesis_endpoint", defaultKinesisEndpoint)
	os.Setenv("AWS_ACCESS_KEY_ID", "not-needed")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "shhh")
	v.Set("aws_region_name", "us-east-1")
	return v
}

func topic(s string) string {
	r := strings.Replace(s, "/", ".", -1)
	suffix := strconv.Itoa(rand.Intn(100000))
	return strings.Join([]string{r, "topic", suffix}, ".")
}

func TestKinesisSink(t *testing.T) {
	suite.Run(t, new(sinkTestSuite))
}

func (s *sinkTestSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
	v := loadKinesisTestENV()

	var err error
	s.kin, err = frinesis.ClientFromViper(v)
	if err != nil {
		s.FailNow("unable to establish connection during test setup", err.Error())
	}

	s.sink, err = frinesis.InitSink(v)
	if err != nil {
		s.FailNow("unable to initialize sink", err.Error())
	}
}

func (s *sinkTestSuite) TearDownSuite() {
}

func (s *sinkTestSuite) SetupTest() {
	s.topic = topic(s.T().Name())
	create := &k.CreateStreamInput{
		StreamName: aws.String(s.topic),
		ShardCount: aws.Int64(1),
	}
	if _, err := s.kin.CreateStream(create); err != nil {
		s.FailNow("unable to create kinesis stream", err.Error())
	}

	// wait for Stream to be active
	resp := &k.DescribeStreamOutput{}
	var err error
	for {
		describe := &k.DescribeStreamInput{
			StreamName: aws.String(s.topic),
		}
		if resp, err = s.kin.DescribeStream(describe); err != nil {
			s.FailNow("DescribeStream failed", err.Error())
		}

		if *resp.StreamDescription.StreamStatus != "ACTIVE" {
			time.Sleep(1 * time.Second)
		} else {
			break
		}

	}
	s.shard = *resp.StreamDescription.Shards[0].ShardId
}

func (s *sinkTestSuite) TearDownTest() {
	// Delete the stream
	if _, err := s.kin.DeleteStream(&k.DeleteStreamInput{StreamName: aws.String(s.topic)}); err != nil {
		s.FailNow("unable to delete kinesis stream", err.Error())
	}
	s.topic = ""
	s.shard = ""
}

// getRecords is based on https://github.com/sendgridlabs/go-kinesis/blob/master/examples/example.go
func (s *sinkTestSuite) getRecords(expectedCount int) []string {
	args := &k.GetShardIteratorInput{
		StreamName:        aws.String(s.topic),
		ShardId:           aws.String(s.shard),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
	}
	resp10, _ := s.kin.GetShardIterator(args)

	shardIterator := *resp10.ShardIterator

	records := []string{}
	for {
		if len(records) >= expectedCount {
			break
		}

		resp11, err := s.kin.GetRecords(&k.GetRecordsInput{ShardIterator: aws.String(shardIterator)})

		if len(resp11.Records) > 0 {
			for _, d := range resp11.Records {
				records = append(records, string(d.Data))
			}
		} else if resp11.NextShardIterator == nil || shardIterator == *resp11.NextShardIterator || err != nil {
			break
		}

		shardIterator = *resp11.NextShardIterator
		time.Sleep(1000 * time.Millisecond)
	}

	return records
}

func (s *sinkTestSuite) sendToSink(data string) {
	msg := frizzle.NewSimpleMsg("foo", []byte(data), time.Now())
	s.sink.Send(msg, s.topic)
}

// stringSliceToMap helps with contents returned out of order
func stringSliceToMap(slice []string) map[string]int {
	sMap := make(map[string]int)
	for _, elem := range slice {
		sMap[elem]++
	}
	return sMap
}

func (s *sinkTestSuite) TestSend() {
	expectedMessages := []string{"time", "to", "test out", "our", "kinesis stream!"}
	receivedMessages := []string{}
	s.T().Log(s.topic)

	for _, m := range expectedMessages {
		s.sendToSink(m)
	}

	if err := s.sink.Close(); err != nil {
		s.Fail("sink close failed", err.Error())
	}

	receivedMessages = s.getRecords(len(expectedMessages))
	s.Equal(stringSliceToMap(expectedMessages), stringSliceToMap(receivedMessages))
}

func (s *sinkTestSuite) TestRestart() {
	expectedMessages := []string{"testing", "a restart", "between", "message sends"}
	receivedMessages := []string{}
	s.T().Log(s.topic)

	s.sendToSink(expectedMessages[0])
	if err := s.sink.Close(); err != nil {
		s.Fail("sink close failed", err.Error())
	}

	if err := s.sink.(*frinesis.Sink).Restart(); err != nil {
		s.Fail("sink restart failed", err.Error())
	}

	for _, m := range expectedMessages[1:] {
		s.sendToSink(m)
	}

	if err := s.sink.(*frinesis.Sink).Restart(); err != nil {
		s.Fail("sink restart failed", err.Error())
	}

	receivedMessages = s.getRecords(len(expectedMessages))
	s.Equal(stringSliceToMap(expectedMessages), stringSliceToMap(receivedMessages))
}
