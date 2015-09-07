package zerorpc

import (
	"fmt"
	"log"
	"reflect"
)

// ZeroRPC client representation,
// it holds a pointer to the ZeroMQ socket
type Client struct {
	socket *socket
}

// Connects to a ZeroRPC endpoint and returns a pointer to the new client
func NewClient(endpoint string) (*Client, error) {
	s, err := connect(endpoint)
	if err != nil {
		return nil, err
	}

	c := Client{
		socket: s,
	}

	return &c, nil
}

// Closes the ZeroMQ socket
func (c *Client) Close() error {
	return c.socket.close()
}

/*
Invokes a ZeroRPC method,
name is the method name,
args are the method arguments

it returns the ZeroRPC response event on success

if the ZeroRPC server raised an exception,
it's name is returned as the err string along with the response event,
the additional exception text and traceback can be found in the response event args

it returns ErrLostRemote if the channel misses 2 heartbeat events,
default is 10 seconds

Usage example:

	package main

	import (
		"fmt"
		"github.com/bsphere/zerorpc"
	)

	func main() {
		c, err := zerorpc.NewClient("tcp://0.0.0.0:4242")
		if err != nil {
			panic(err)
		}

		defer c.Close()

		response, err := c.Invoke("hello", "John")
		if err != nil {
			panic(err)
		}

		fmt.Println(response)
	}

It also supports first class exceptions, in case of an exception,
the error returned from Invoke() or InvokeStream() is the exception name
and the args of the returned event are the exception description and traceback.

The client sends heartbeat events every 5 seconds, if twp heartbeat events are missed,
the remote is considered as lost and an ErrLostRemote is returned.
*/
func (c *Client) Invoke(name string, args ...interface{}) ([]interface{}, error) {
	log.Printf("ZeroRPC client invoked %s with args %s", name, args)

	ev, err := newEvent(name, args)
	if err != nil {
		return nil, err
	}

	ch := c.socket.newChannel("")
	defer ch.close()

	err = ch.sendEvent(ev)
	if err != nil {
		return nil, err
	}

	for {
		select {
		case response := <-ch.channelOutput:
			if response.Name == "ERR" {
				return response.Args, fmt.Errorf("%s", response.Args)
			} else {
				return response.Args, nil
			}

		case err := <-ch.channelErrors:
			return nil, err
		}
	}
}

func (c *Client) InvokeReply(name string, reply interface{}, args ...interface{}) (err error) {

	val := reflect.ValueOf(reply).Elem()
	typ := reflect.TypeOf(reply)

	if typ.Kind() != reflect.Ptr {
		err = fmt.Errorf("reply must be pointer, get %s %s", val.Kind(), val)
		return
	}
	typ = typ.Elem()

	reply_args, err := c.Invoke(name, args...)
	if err != nil || typ.NumField() != len(reply_args) {
		return
	}

	for i := 0; i < typ.NumField(); i++ {

		f := val.Field(i)

		if !f.CanSet() {
			continue
		}

		r := reply_args[i]

		switch f.Kind() {
		case reflect.Int:
			f.SetInt(r.(int64))
		case reflect.String:
			f.SetString(r.(string))
		}
	}
	return
}

/*
Invokes a streaming ZeroRPC method,
name is the method name,
args are the method arguments

it returns an array of ZeroRPC response events on success

if the ZeroRPC server raised an exception,
it's name is returned as the err string along with the response event,
the additional exception text and traceback can be found in the response event args

it returns ErrLostRemote if the channel misses 2 heartbeat events,
default is 10 seconds

Usage example:

	package main

	import (
		"fmt"
		"github.com/bsphere/zerorpc"
	)

	func main() {
		c, err := zerorpc.NewClient("tcp://0.0.0.0:4242")
		if err != nil {
			panic(err)
		}

		defer c.Close()

		response, err := c.InvokeStream("streaming_range", 10, 20, 2)
		if err != nil {
			fmt.Println(err.Error())
		}

		for _, r := range response {
			fmt.Println(r)
		}
	}

It also supports first class exceptions, in case of an exception,
the error returned from Invoke() or InvokeStream() is the exception name
and the args of the returned event are the exception description and traceback.

The client sends heartbeat events every 5 seconds, if two heartbeat events are missed,
the remote is considered as lost and an ErrLostRemote is returned.
*/
func (c *Client) InvokeStream(name string, args ...interface{}) ([][]interface{}, error) {
	log.Printf("ZeroRPC client invoked %s with args %s in streaming mode", name, args)

	ev, err := newEvent(name, args)
	if err != nil {
		return nil, err
	}

	ch := c.socket.newChannel("")
	defer ch.close()

	err = ch.sendEvent(ev)
	if err != nil {
		return nil, err
	}

	out := make([][]interface{}, 0)

	for {
		select {
		case response := <-ch.channelOutput:
			if response.Name == "ERR" {
				out = append(out, response.Args)
				return out, fmt.Errorf("%s", response.Args)
			} else if response.Name == "OK" {
				out = append(out, response.Args)
				return out, nil
			} else if response.Name == "STREAM" {
				out = append(out, response.Args)
			} else {
				return out, nil
			}

		case err := <-ch.channelErrors:
			return nil, err
		}
	}
}
