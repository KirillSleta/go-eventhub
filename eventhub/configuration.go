package eventhub

type AzEventHubConfiguration struct {
	eventHubNamespace  string
	eventHubName       string
	eventHubSasKeyName string
	eventHubSasKey     string
}
