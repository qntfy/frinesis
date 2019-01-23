Kinesis `batchproducer` was forked from [sendgridlabs/go-kinesis](https://github.com/sendgridlabs/go-kinesis) 
with the following significant modifications by Qntfy:
* Use canonical AWS SDK Kinesis client instead of custom client written by sendgridlabs
* Use [zap](https://github.com/uber-go/zap) for logging
* Expose an `Events()` channel for asynchronous reporting of errors
