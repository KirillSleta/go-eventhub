package eventhub

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/Azure/go-autorest/autorest"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

//ErrSubscriptionRequired is raised when a service bus topics operations is performed without a subscription.
var (
	ErrSubscriptionRequired = fmt.Errorf("A subscription is required for Service Bus Topic operations")
)

//ClientType denotes the type of client (topic/queue) that is required
type ClientType int

const (
	//Queue is a client type for Azure Service Bus queues
	Queue ClientType = iota

	//Topic is a client type for Azure Service Bus topics
	Topic                ClientType = iota
	defaultRetryAttempts            = 5
	defaultRetryDuration            = time.Second * 1
)

//EventHubClient is a client for Azure Service Bus (queues and topics). You should use a different client instance for every namespace.
//For more comprehensive documentation on its various methods, see:
//	https://msdn.microsoft.com/en-us/library/azure/hh780717.aspx
type EventHubClient interface {
	DeleteMessage(item *Message) error
	PeekLockMessage(path string, timeout int) (*Message, error)
	CreateEventHub(path string) error
	Send(path string, item *Message) error
	SendBatch(path string, items []*BatchMessage) error
	SetSubscription(subscription string)
	//Unlock(item *Message) error
}

//client is the default implementation of Client
type client struct {
	clientType        ClientType
	eventHubNamespace string

	subscription     string
	saKey            string
	saValue          string
	url              string
	client           *http.Client
	RetryAttempts    int
	RetryDuration    time.Duration
	ValidStatusCodes []int
	attempts         int // used for testing
}

const (
	serviceBusURL    = "https://%s.servicebus.windows.net:443/"
	apiVersion       = "2016-07"
	batchContentType = "application/vnd.microsoft.servicebus.json"
)

var (
	defaultValidStatusCodes = []int{
		http.StatusRequestTimeout,      // 408
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout,      // 504
	}
)

//New creates a new client from the given parameters. Their meaning can be found in the MSDN docs at:
//  https://docs.microsoft.com/en-us/rest/api/servicebus/Introduction
func newClient(clientType ClientType, eventHubNamespace string, sharedAccessKeyName string, sharedAccessKeyValue string) *client {
	client := &client{
		clientType:        clientType,
		eventHubNamespace: eventHubNamespace,
		saKey:             sharedAccessKeyName,
		saValue:           sharedAccessKeyValue,
		url:               fmt.Sprintf(serviceBusURL, eventHubNamespace),
		client:            &http.Client{},
		RetryAttempts:     defaultRetryAttempts,
		ValidStatusCodes:  defaultValidStatusCodes,
		RetryDuration:     defaultRetryDuration,
	}
	return client
}

//NewEventHubClientFromConfig creates a new client from the given parameters. Their meaning can be found in the MSDN docs at:
//  https://docs.microsoft.com/en-us/rest/api/servicebus/Introduction
func NewEventHubClientFromConfig(clientType ClientType, config AzEventHubConfiguration) EventHubClient {
	return newClient(clientType, config.eventHubNamespace, config.eventHubSasKeyName, config.eventHubSasKey)
}

//NewEventHubClient creates a new client from the given parameters. Their meaning can be found in the MSDN docs at:
//  https://docs.microsoft.com/en-us/rest/api/servicebus/Introduction
func NewEventHubClient(clientType ClientType, namespace string, sharedAccessKeyName string, sharedAccessKeyValue string) EventHubClient {
	return newClient(clientType, namespace, sharedAccessKeyName, sharedAccessKeyValue)
}

// Send is the default retry strategy in the client
func (ds *client) sendWithRetry(req *http.Request) (resp *http.Response, err error) {
	rr := autorest.NewRetriableRequest(req)
	for attempts := 0; attempts < ds.RetryAttempts; attempts++ {
		err = rr.Prepare()
		if err != nil {
			return resp, err
		}
		resp, err = ds.client.Do(rr.Request())
		if err != nil || !autorest.ResponseHasStatusCode(resp, ds.ValidStatusCodes...) {
			return resp, err
		}
		autorest.DelayForBackoff(ds.RetryDuration, attempts, req.Cancel)
		ds.attempts = attempts
	}
	ds.attempts++
	return resp, err
}

//SetSubscription sets the client's subscription. Only required for Azure Service Bus Topics.
func (c *client) SetSubscription(subscription string) {
	c.subscription = subscription
}

func (c *client) request(url string, method string) (*http.Request, error) {
	return c.requestWithBody(url, method, nil)
}

func (c *client) requestWithBody(urlString string, method string, body []byte) (*http.Request, error) {

	url, err := url.Parse(urlString)
	if err != nil {
		return nil, err
	}
	q := url.Query()
	q.Set("api-version", apiVersion)
	url.RawQuery = q.Encode()

	req, err := http.NewRequest(method, url.String(), bytes.NewBuffer(body)) // TODO: handle existing query params
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", c.authHeader(url.String(), c.signatureExpiry(time.Now())))
	return req, nil
}

//DeleteMessage deletes the message.
//
//For more information see https://docs.microsoft.com/en-us/rest/api/servicebus/delete-message.
func (c *client) DeleteMessage(item *Message) error {
	req, err := c.request(item.Location, "DELETE")

	if err != nil {
		return err
	}

	resp, err := c.sendWithRetry(req)

	if err != nil {
		return err
	}

	io.Copy(ioutil.Discard, resp.Body)

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	return readError(resp)
}

//Creates an eventhub in the given namespace.
//
//For more information see https://docs.microsoft.com/en-us/rest/api/eventhub/create-event-hub
func (c *client) CreateEventHub(path string) error {

	//TODO: modify eventhub definition according yours specific needs
	xmlAtom := "<entry xmlns='http://www.w3.org/2005/Atom'><content type='application/xml'><EventHubDescription xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"http://schemas.microsoft.com/netservices/2010/10/servicebus/connect\"></EventHubDescription></content></entry>"

	req, err := c.requestWithBody(c.url+path, "PUT", []byte(xmlAtom))
	req.Header.Set("Content-Type", "application/atom+xml;type=entry;charset=utf-8")

	if err != nil {
		return err
	}

	resp, err := c.sendWithRetry(req)

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusCreated ||
		resp.StatusCode == http.StatusConflict {
		io.Copy(ioutil.Discard, resp.Body)
		return nil
	}

	return readError(resp)
}

//Send sends a new item to `path`, where `path` is either the queue name or the topic name.
//
//For more information see https://docs.microsoft.com/en-us/rest/api/servicebus/send-message-to-queue.
func (c *client) Send(path string, item *Message) error {
	req, err := c.requestWithBody(c.url+path+"/messages/", "POST", item.Body)

	if err != nil {
		return err
	}

	resp, err := c.sendWithRetry(req)

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		io.Copy(ioutil.Discard, resp.Body)
		return nil
	}

	return readError(resp)
}

//Sends a new batched message event to an Event Hub.
// Batching reduces the number of messages that are transmitted by merging information
// from multiple messages into a single batch of messages. This reduces the number of
// connections established, and reduces network bandwidth by reducing the number of
// packet headers that are sent over the network.
//
//For more information see https://docs.microsoft.com/en-us/rest/api/eventhub/send-batch-events.
//TODO: ensure that the message is up to 256Kb, divide messages if larger than that limit
func (c *client) SendBatch(path string, items []*BatchMessage) error {

	bytes, err := json.Marshal(items)
	req, err := c.requestWithBody(c.url+path+"/messages/", "POST", bytes)
	req.Header.Set("Content-Type", batchContentType)

	if err != nil {
		return err
	}

	resp, err := c.sendWithRetry(req)

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		io.Copy(ioutil.Discard, resp.Body)
		return nil
	}

	return readError(resp)
}

//Unlock unlocks a message for processing by other receivers.
//
//For more information see https://docs.microsoft.com/en-us/rest/api/servicebus/unlock-message.
func (c *client) Unlock(item *Message) error {
	req, err := c.request(item.Location+"/"+item.LockToken, "PUT")

	if err != nil {
		return err
	}

	resp, err := c.sendWithRetry(req)

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusOK {
		io.Copy(ioutil.Discard, resp.Body)
		return nil
	}

	return readError(resp)
}

//PeekLockMessage atomically retrieves and locks the latest message from the queue or topic at `path` (which should not include slashes).
//
//If using this with a service bus topic, make sure you SetSubscription() first.
//For more information see https://docs.microsoft.com/en-us/rest/api/servicebus/peek-lock-message-non-destructive-read.
func (c *client) PeekLockMessage(path string, timeout int) (*Message, error) {
	var url string
	if c.clientType == Queue {
		url = c.url + path + "/"
	} else {
		if c.subscription == "" {
			return nil, ErrSubscriptionRequired
		}
		url = c.url + path + "/subscriptions/" + c.subscription + "/"
	}
	req, err := c.request(url+fmt.Sprintf("messages/head?timeout=%d", timeout), "POST")

	if err != nil {
		return nil, err
	}
	resp, err := c.sendWithRetry(req)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNoContent {
		io.Copy(ioutil.Discard, resp.Body)
		return nil, nil
	}

	if resp.StatusCode != http.StatusCreated {
		return nil, readError(resp)
	}

	defer resp.Body.Close()
	mBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Error reading message body")
	}

	brokerProperties := resp.Header.Get("BrokerProperties")

	location := resp.Header.Get("Location")

	var message Message

	if err := json.Unmarshal([]byte(brokerProperties), &message); err != nil {
		return nil, fmt.Errorf("Error unmarshalling BrokerProperties: %v", err)
	}

	message.Location = location
	message.Body = mBody

	return &message, nil
}

//signatureExpiry returns the expiry for the shared access signature for the next request.
//
//It's translated from the Python client:
// https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/azure/servicebus/servicebusservice.py
func (c *client) signatureExpiry(from time.Time) string {
	t := from.Add(300 * time.Second).Round(time.Second).Unix()
	return strconv.Itoa(int(t))
}

//signatureURI returns the canonical URI according to Azure specs.
//
//It's translated from the Python client:
//https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/azure/servicebus/servicebusservice.py
func (c *client) signatureURI(uri string) string {
	return strings.ToLower(url.QueryEscape(uri)) //Python's urllib.quote and Go's url.QueryEscape behave differently. This might work, or it might not...like everything else to do with authentication in Azure.
}

//stringToSign returns the string to sign.
//
//It's translated from the Python client:
//https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/azure/servicebus/servicebusservice.py
func (c *client) stringToSign(uri string, expiry string) string {
	return uri + "\n" + expiry
}

//signString returns the HMAC signed string.
//
//It's translated from the Python client:
//https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/azure/servicebus/_common_conversion.py
func (c *client) signString(s string) string {
	h := hmac.New(sha256.New, []byte(c.saValue))
	h.Write([]byte(s))
	encodedSig := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return url.QueryEscape(encodedSig)
}

//AuthHeader returns the value of the Authorization header for requests to Azure Service Bus.
//
//It's translated from the Python client:
//https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/azure/servicebus/servicebusservice.py
func (c *client) authHeader(uri string, expiry string) string {
	u := c.signatureURI(uri)
	s := c.stringToSign(u, expiry)
	sig := c.signString(s)
	return fmt.Sprintf("SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s", sig, expiry, c.saKey, u)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
