package zerorpc

import "testing"

type reply struct {
	Word string
}

func TestInvokeReply(t *testing.T) {

	r := new(reply)

	s, err := NewServer("inproc://invoke-reply")
	if err != nil {
		t.Error(err)
	}
	s.RegisterTask("hello", func(v []interface{}) (interface{}, error) { return "Hello," + v[0].(string), nil })
	go s.Listen()

	c, err := NewClient("inproc://invoke-reply")
	err = c.InvokeReply("hello", r, "World")
	if err != nil || r.Word != "Hello,World" {
		t.Errorf("Request response failed, expecting Hello,World, get%#v, err=%s", r, err)
	}
}
