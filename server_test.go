package zerorpc

import "testing"

func TestServerRegister(t *testing.T) {
	s, err := NewServer("inproc://server-register")
	defer s.Close()
	if err != nil {
		panic(err)
	}

	defer s.Close()

	h := func(v []interface{}) (interface{}, error) {
		return "Hello, " + v[0].(string), nil
	}

	s.RegisterTask("hello", h)
	err = s.RegisterTask("hello", h)
	if err == nil {
		t.Error("Duplicate registered")
	}
}

func TestRequestResponse(t *testing.T) {
	s, err := NewServer("inproc://request-response")
	defer s.Close()
	if err != nil {
		t.Error(err)
	}
	s.RegisterTask("hello", func(v []interface{}) (interface{}, error) { return "Hello," + v[0].(string), nil })
	go s.Listen()

	c, err := NewClient("inproc://request-response")
	defer c.Close()
	reply, err := c.Invoke("hello", "World")
	if reply[0].(string) != "Hello,World" {
		t.Errorf("Request response failed, expecting Hello,World, get%s", reply)
	}
}
