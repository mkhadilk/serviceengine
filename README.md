# serviceengine
Golang based Consumer-Producer Engine

This library allows the simple in memory way to produce messages to a topic/s and allows the consumers to be set to receive messages, either from a single topic or multiple.

## Introduction
Service Engine is a typical Pub/Sub way to request a service, optionally wait for a reponse, write a code to be invoked based on a Request and send results back to the requestor.

ENGINE instance is created first time the import of this package happens.

## A typical way to use the library
Usually, a dependent package imports the service engine initilizing the ENGINE. Then, we can create a Consumer like below:

```
nc := NewConsumer(func(request Request) {
	log.Printf("message is %+v", request)
  // Do something and prepare a response......
	res := NewResponse(request.GetID(), request.ExternalID)
	request.Respond(*res)
	return
})
```
Now, add this consumer to listen on a topic: (One can query the engine if there is already a consumer for a topic by HasConsumer method).

```
ENGINE.AddConsumer("a newtopic", *nc)
```
Then, somehere in your code we can create a Request - 
```
nreq := NewRequest("mybadid")
nreq.Message = []byte("some message")
```
And now, send the request for execution like:
```
ENGINE.Execute("a newtopic", *nreq)
```
You will notice that the Consumer code will get called and it would reponse to Resonse channel inside the Request object.

One can read the response in two ways:
Just do-
```
ress := nreq.Receive()
```
OR

We can create a seperate Response Channel and as in-
```
var reschan chan Response
//buffer capacity of 10
reschan = make(chan Response, 10)
```
And create a Request with this common channel-
```
nreq := NewRequestWith("mybadid", reschan)
```
In that case, one can invoke a Go routing to listen to all responses as in-
```
go func(reschan <-chan Response, resn int) {
	n := 0
	for {
		if n == resn {
			log.Printf("Received %d", n)
			return
		}
		_, ok := <-reschan
		if ok {
			n++
		} else {
			log.Printf("Received %d", n)
			return
		}
	}
}(reschan, sentcount)
```
 This is a sample code which exits either when all (sentcount) messages are received or Response Channel is closed.

 ## More complex cases are discussed in Test Cases.
