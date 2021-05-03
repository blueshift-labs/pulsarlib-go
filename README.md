## About Pulsar 

Pulsar is a distributed pub-sub messaging platform with a very
flexible messaging model and an intuitive client API.

Learn more about Pulsar at https://pulsar.apache.org

## About this library

This library is a wrapper on top of [pulsar-client-go](github.com/apache/pulsar-client-go)
It makes producing and consuming from pulsar topics very easy and abstracts all the complexities. Additionally this library provides Admin API for pulsar administration.

## Usage 
Add the following value to GOPRIVATE
Without this the build might fail.
```
export GOPRIVATE="github.com/blueshift-labs:$GOPRIVATE"
```
Import the following module
```go
import "github.com/blueshift-labs/pulsarlib-go/pulsarlib"
```
Initialize the library. This need to be done only once in the whole lifecycle of your process
```go
worker_count := 10 //Number of consumer goroutines
pulsar_host := "pulsar.local" //pulsar host url
err := pulsarlib.InitMessaging(worker_count, pulsar_host)
if err != nil {
    panic("Error initializing the library")
}
```

### Publishing messages
Create the Producer using the following code
```go
tanantID := "my-tenant" //Name of the tenant
namespace := "my_namespace" //Name of the namespace
topic := "my_topic" //Topic to which the messages need to be published
producer, err := pulsarlib.CreateProducer(tenantID, namespace, topic)
if err != nil {
    panic("Unable to create producer")
}

//Two dummy messages
messages := []*pulsarlib.Message{
    &pulsarlib.Message{
        Key: "message-1",
        Value: "This is the first message",
        Properties: map[string]string{
            "message-type": "string"
        }
    },
    &pulsarlib.Message{
        Key: "message-2",
        Value: "This is the second message",
        Properties: map[string]string{
            "message-type": "string"
        }
    }
}

//Publish the messages
err = producer.Publish(messages)
if err != nil {
    panic("Error publishing messages")
}

stats := producer.Stats()
fmt.Println("Published messages stats:", stats)

//Cleanup the producer
producer.Stop()
```

### Consuming messages
Create the consumer using the following code
```go

//Create a handler. It needs to implement pulsarlib.Handler interface.
type myHandler struct{}
func (h *myHandler) HandleMessage(msg *pulsarlib.Message) {
    fmt.Printf("Message consumed. Key [%s] Value [%v] Properties [%v]", msg.Key, msg.Value, msg.Properties)
}  

tanantID := "my-tenant" //Name of the tenant
namespace := "my_namespace" //Name of the namespace
topics := []string{"my-topic1"} //Can consume from multiple topics within a namespace
subscriptionName := "my-subscription"
handler := &myHandler{}

consumer, err := pulsarlib.CreateConsumer(tenantID string, namespace string, topics []string, subscriptionName string, handler Handler)
if err != nil {
    panic("Error publishing messages")
}

//Start the consumer
err = consumer.Start()
if err != nil {
    panic("Error starting consumer")
}

//To pause the consumer
consumer.Pause()

//To unpause the consumer
consumer.unpause()

//Stopping the consumer
err = consumer.Stop()
if err != nil {
    panic("Error stopping consumer")
}

//If you want to delete the subscription
err = consumer.Unsubscribe()
if err != nil {
    panic("Error unsubscribing")
}

stats := consumer.Stats()
fmt.Println("Consumed messages stats:", stats)
```

### AdminAPIs
The following admin APIs are provided by this library
```go
//Available APIs
CreateTenant(tenantID string, adminRoles []string, allowedClusters []string) error
DeleteTenant(tenantID string) error
CreateNamespace(tenantID string, namespace string) error
DeleteNamespace(tenantID string, namespace string) error
UnloadNamespace(tenantID string, namespace string) error
ListTenants() ([]string, error)
ListNamespaces(tenantID string) ([]string, error)
CreateTopic(tenantID string, namespace string, topic string) error
DeleteTopic(tenantID string, namespace string, topic string) error
ListTopics(tenantID string, namespace string) ([]string, error)
CreatePartitionedTopic(tenantID string, namespace string, topic string, partitions int) error
DeletePartionedTopic(tenantID string, namespace string, topic string) error
ListPartionedTopics(tenantID string, namespace string) ([]string, error)
```
To use the APIs just call the function with required parameters. For example
```go
//ListTenants
pulsarTenants, err := pulsarlib.ListTenants()
if err != nil {
    panic("Unable to list tenants")
}
```