package frinesis

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// generateID generates a unique ID for a Msg
func generateID() string {
	id, _ := uuid.NewV4()
	return id.String()
}

// ClientFromViper takes a viper config and returns an initialized go-kinesis client.
// Used internally and as a test helper method.
func ClientFromViper(config *viper.Viper) (*kinesis.Kinesis, error) {
	if !config.IsSet("aws_region_name") {
		return nil, errors.New("following config fields are required for kinesis sink: aws_region_name")
	}

	var client *kinesis.Kinesis
	if !config.IsSet("kinesis_endpoint") {
		// If no endpoint set, running in live AWS and should have valid credentials available on the default chain
		client = NewClient(config.GetString("aws_region_name"))
	} else {
		endpoint := config.GetString("kinesis_endpoint")
		// accepts endpoint with or without scheme prefix, defaults to http for testing if not provided
		if !strings.HasPrefix(endpoint, "http") {
			endpoint = strings.Join([]string{"http://", endpoint}, "")
		}
		// create client with dummy auth since we are using a manual endpoint
		client = NewClientWithEndpoint(
			config.GetString("aws_region_name"),
			endpoint,
		)
	}

	return client, nil
}

// NewClient returns a default AWS SDK kinesis client
func NewClient(region string) *kinesis.Kinesis {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	return kinesis.New(sess)
}

// NewClientWithEndpoint returns an AWS SDK kinesis client pointing to a custom service endpoint
func NewClientWithEndpoint(region, endpoint string) *kinesis.Kinesis {
	customResolver := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		if service == endpoints.KinesisServiceID {
			return endpoints.ResolvedEndpoint{
				URL:           endpoint,
				SigningRegion: region,
			}, nil
		}

		return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
	}
	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(region),
		EndpointResolver: endpoints.ResolverFunc(customResolver),
	}))
	return kinesis.New(sess)
}
