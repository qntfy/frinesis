# frinesis

An AWS Kinesis implementation of a [Frizzle](https://github.com/qntfy/frizzle) Sink.

In addition to the AWS Kinesis SDK for Go, Frinesis uses a modified version of
[sendgridlabs/go-kinesis/batchproducer](https://github.com/sendgridlabs/go-kinesis) (under separate MIT license).

Frizzle is a magic message (`Msg`) bus designed for parallel processing w many goroutines.
  * `Receive()` messages from a configured `Source`
  * Do your processing, possibly `Send()` each `Msg` on to one or more `Sink` destinations
  * `Ack()` (or `Fail()`) the `Msg`  to notify the `Source` that processing completed

## Running the tests

`go test -v --cover ./...`

## Configuration
Frinesis Sinks are configured using [Viper](https://godoc.org/github.com/spf13/viper).
```
func InitSink(config *viper.Viper) (*Sink, error)

InitSinkWithLogger(config *viper.Viper, logger *zap.Logger)
```

We typically initialize Viper through environment variables (but client can do whatever it wants,
just needs to provide the configured Viper object with relevant values). The application might
use a prefix before the below values.

| Variable | Required | Description | Default |
|---------------------------|:--------:|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-------:|
| AWS_REGION_NAME | required | region being used e.g. `us-east-1` |  |
| KINESIS_ENDPOINT | optional | if using a custom endpoint e.g. for local testing. Defaults to AWS standard internal and retrieving credentials from IAM if not set. `http://` prefixed if no scheme set |  |
| KINESIS_FLUSH_TIMEOUT | sink (optional) | how long to wait for Kinesis Sink to flush remaining messages on close (use duration) | `30s` |

## Async Error Handling
Since records are sent in batch fashion, Kinesis may report errors asynchronously.
Errors can be recovered via channel returned by the `Sink.Events()` method.
In addition to the `String()` method required by frizzle, currently only errors are
returned by frinesis (no other event types) so all Events recovered will also conform
to `error` interface.

## Contributing
Contributions welcome! Take a look at open issues.
