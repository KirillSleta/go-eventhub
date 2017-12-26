# Azure EventHub Golang REST client

Azure EventHub client library uses an Azure EventHub (ServiceBus) REST endpoints. 
The motivation behind this code is absense of simple, not-AMQP, but REST-based client for Azure EventHub

Available operations:
- CreateEventHub
- Send
- SendBatch (be careful - there is 256Kb limitation should be handled by you)
- PeekLockMessage - peek and lock message for processing
- SetSubscription - target subscription for peeklock
- DeleteMessage

# Installation
- If you don't already have it, install [the Go Programming Language](https://golang.org/dl/).
- Go get the [Autorest](https://github.com/Azure/autorest) package:

```
$ go get -u github.com/Azure/go-autorest/autorest
```
- Go get the lib:

```
$ go get -u github.com/KirillSleta/go-eventhub
```


# Code sample

```
func sendData(client EventHubClient, hub string, data interface{}) error {
	payload, err := convertDataToJsonPayload(data)
	if err != nil {
		return err
	}
	client.Send(hub, &eventhub.Message{Body: payload})

	return nil
}
```

# License

This project is published under [Apache 2.0 License](LICENSE).
