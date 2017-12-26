package eventhub

import "fmt"

type FakeEventHubClient struct {
}

func (c *FakeEventHubClient) Send(path string, item *Message) error {
	fmt.Println("Send")
	return nil
}
func (c *FakeEventHubClient) SendBatch(path string, items []*BatchMessage) error {
	fmt.Println("SendBatch")
	return nil
}

func (c *FakeEventHubClient) CreateEventHub(path string) error {
	fmt.Println("CreateEventHub")
	return nil
}
