package executer

import (
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestBuildConfig(t *testing.T) {
	var bn BuildInfo

	bn.Service.CommitId = "somecommit"
	bn.Service.Name = "service"
	bn.Service.Version = "0.0.1"
	bn.Components = make([]struct {
		Name     string `json:"name"`
		Version  string `json:"version"`
		CommitId string `json:"commitId"`
	}, 0)
	bn.Components = append(bn.Components, struct {
		Name     string `json:"name"`
		Version  string `json:"version"`
		CommitId string `json:"commitId"`
	}{
		Name:     "c1",
		Version:  "v1",
		CommitId: "cm1",
	})

	log.Printf("build info is %+v", bn)

	jsonbytes, err := json.MarshalIndent(bn, " ", " ")

	if err != nil {
		log.Printf("error %+v", err)
		t.FailNow()
	}

	jsonbytes, err = json.MarshalIndent(bn, " ", " ")

	if err != nil {
		log.Printf("error %+v", err)
		t.FailNow()
	}

	log.Printf("local build JSON %s", string(jsonbytes))

	jsonbytes, err = json.MarshalIndent(BUILDINFO, " ", " ")

	if err != nil {
		log.Printf("error %+v", err)
		t.FailNow()
	}

	log.Printf("main build JSON %s", jsonbytes)

}

func TestEngineRun(t *testing.T) {
	nreq := NewRequest("mybadid")
	nreq.Message = []byte("some message")

	nc := NewConsumer(func(request Request) {
		log.Printf("message is %+v", request)
		res := NewResponse(request.GetID(), request.ExternalID)
		request.Respond(*res)
		return
	})
	ENGINE.AddConsumer("a newtopic", *nc)

	if !ENGINE.HasConsumers("a newtopic") {
		t.FailNow()
	} else {
		cc := ENGINE.GetConsumers("a newtopic")
		if len(cc) == 0 {
			t.FailNow()
		} else {
			if nc.GetID() != cc[0].GetID() {
				t.FailNow()
			}
		}
	}

	log.Println("Consumer added")

	ENGINE.Execute("a newtopic", *nreq)
	log.Println("Executed")

	ress := nreq.Receive()

	if nreq.GetID() != ress.GetID() {
		log.Printf("Request and Response Ids do not match - %s and %s", nreq.GetID(), ress.GetID())
	}

	log.Printf("Res is %+v", ress)

}

func TestEngineRunRepeatRequest(t *testing.T) {
	nreq := NewRequest("mybadid")
	nreq.Message = []byte("some message")

	nc := NewConsumer(func(request Request) {
		log.Printf("message is %+v", request)
		res := NewResponse(request.GetID(), request.ExternalID)
		request.Respond(*res)
		return
	})
	ENGINE.AddConsumer("a newtopic", *nc)

	if !ENGINE.HasConsumers("a newtopic") {
		log.Println("Consumer not found")
		t.FailNow()
	} else {
		cc := ENGINE.GetConsumers("a newtopic")
		if len(cc) == 0 {
			log.Println("Can not get Consumers")
			t.FailNow()
		}
	}

	log.Println("Consumer added")

	ENGINE.Execute("a newtopic", *nreq)
	log.Println("Executed")

	ress := nreq.Receive()

	if nreq.GetID() != ress.GetID() {
		log.Printf("Request and Response Ids do not match - %s and %s", nreq.GetID(), ress.GetID())
	}

	log.Printf("Res is %+v", ress)

	err := ENGINE.Execute("a newtopic", *nreq)
	if err == nil {
		log.Println("Executed")
	} else {
		t.FailNow()
	}

	ress = nreq.Receive()

	if nreq.GetID() != ress.GetID() {
		log.Printf("Request and Response Ids do not match - %s and %s", nreq.GetID(), ress.GetID())
	}

	log.Printf("Res is %+v", ress)

}

func TestEngineScaleRunCommonResponse(t *testing.T) {
	nreq := NewRequest("mybadid")
	nreq.Message = []byte("some message")

	nc := NewConsumer(func(request Request) {
		res := NewResponse(request.GetID(), request.ExternalID)
		request.Respond(*res)
		return
	})
	ENGINE.AddConsumer("a newtopic1", *nc)

	log.Println("Consumer added")

	sentcount := 1000
	var reschan chan Response

	reschan = make(chan Response, 10)

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

	for i := 0; i < sentcount; i++ {
		nreq := NewRequestWith("mybadid", reschan)
		nreq.Message = Message("some message " + strconv.Itoa(i))
		ENGINE.Execute("a newtopic1", *nreq)
	}

	log.Printf("Sent messages %d", sentcount)

	ENGINE.Stop()
	select {
	case nc.Exit <- true:
	case <-time.After(time.Millisecond * 100):
	}
}

func TestEngineScaleRunResponse(t *testing.T) {
	nreq := NewRequest("mybadid")
	nreq.Message = []byte("some message")

	var rchans chan (<-chan Response)

	rchans = make(chan (<-chan Response), 1)

	var rg sync.WaitGroup
	rg.Add(1)
	go ResponseFunc(rchans, &rg)

	nc := NewConsumer(func(request Request) {
		res := NewResponse(request.GetID(), request.ExternalID)
		request.Respond(*res)
		return
	})
	ENGINE.AddConsumer("a newtopic2", *nc)

	log.Println("Consumer added")

	sentcount := 1000

	for i := 0; i < sentcount; i++ {
		nreq := NewRequest("mybadid")
		nreq.Message = Message("some message " + strconv.Itoa(i))
		rchans <- nreq.GetReceiver()
		ENGINE.Execute("a newtopic2", *nreq)
	}
	log.Printf("Sent messages %d", sentcount)
	close(rchans)
	rg.Wait()
	nc.Exit <- true

	ENGINE.Stop()
}

func TestEngineUnknownTopic(t *testing.T) {
	nreq := NewRequest("mybadid")
	nreq.Message = []byte("some message")

	err := ENGINE.Execute("a newtopic2", *nreq)

	if err == nil {
		t.Fail()
	}

	ENGINE.Stop()
}

func TestEngineAddRemoveConsumer(t *testing.T) {
	nreq := NewRequest("mybadid")
	nreq.Message = []byte("some message")

	err := ENGINE.Execute("a newtopic2", *nreq)

	if err == nil {
		t.Fail()
	}

	if ENGINE.HasConsumers("a newtopic2") {
		t.Fail()
	}

	nc := NewConsumer(func(request Request) {
		res := NewResponse(request.GetID(), request.ExternalID)
		request.Respond(*res)
		return
	})
	nc1 := NewConsumer(func(request Request) {
		res := NewResponse(request.GetID(), request.ExternalID)
		request.Respond(*res)
		return
	})

	if ENGINE.RemoveConsumer("a newtopic2", nc.GetID()) {
		t.Fail()
	}

	ENGINE.AddConsumer("a newtopic2", *nc)

	if !ENGINE.HasConsumers("a newtopic2") {
		t.Fail()
	}

	if !ENGINE.RemoveConsumer("a newtopic2", nc.GetID()) {
		t.Fail()
	}

	if ENGINE.HasConsumers("a newtopic2") {
		t.Fail()
	}

	ENGINE.AddConsumer("a newtopic2", *nc)
	ENGINE.AddConsumer("a newtopic2", *nc1)

	if !ENGINE.HasConsumers("a newtopic2") {
		t.Fail()
	}

	if !ENGINE.RemoveConsumer("a newtopic2", nc.GetID()) {
		t.Fail()
	}

	if !ENGINE.HasConsumers("a newtopic2") {
		t.Fail()
	}

	if len(ENGINE.GetConsumers("a newtopic2")) != 1 {
		t.Fail()
	}

	if !ENGINE.RemoveConsumer("a newtopic2", nc1.GetID()) {
		t.Fail()
	}

	if len(ENGINE.GetConsumers("a newtopic2")) != 0 {
		t.Fail()
	}

	ENGINE.Stop()
}

func ResponseFunc(rchans <-chan <-chan Response, done *sync.WaitGroup) {
	n := 0
	for {
		rc, ok := <-rchans
		if ok {
			_, ok := <-rc
			if ok {
				n++
			}
		} else {
			log.Printf("Received %d messages", n)
			done.Done()
			return
		}
	}
}

func TestMessageBody(t *testing.T) {
	var m MessageBody
	i := struct {
		Ii int
		Pp string
	}{
		Ii: 3,
		Pp: "pppp",
	}

	err := m.AsJSON(&i)

	if err != nil {
		log.Printf("error %+v", err)
		t.Fail()
	}

	var iii struct {
		Ii int
		Pp string
	}

	err = m.FromJSONInto(&iii)
	if err != nil {
		log.Printf("error %+v", err)
		t.Fail()
	}

	if i.Ii != iii.Ii || i.Pp != iii.Pp {
		log.Printf("error input %+v not matching output %+v", i, iii)
		t.Fail()
	}
}
