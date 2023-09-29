package executer

import (
	_ "embed"
	"encoding/json"
	_ "expvar"
	"log"
	"os"

	"github.com/google/uuid"
)

//go:embed build.conf
var BUILDJSON []byte

var BUILDINFO BuildInfo

var requestBufferSize int = 1  // buffered for single request; asynch
var responseBufferSize int = 0 // unbuffered; synchronous response

var ENGINE Engine

type Message []byte

type Status int

const (
	StatusNew       Status = iota
	StatusSent      Status = iota
	StatusReceived  Status = iota
	StatusError     Status = iota
	StatusCompleted Status = iota
)

func init() {
	log.SetFlags(log.LstdFlags | log.Llongfile)
	err := json.Unmarshal(BUILDJSON, &BUILDINFO)
	if err != nil {
		log.Printf("Build info not available %+v", err)
		os.Exit(-1)
	}

	ENGINE = NewEngine()
}

type MessageBody struct {
	id         uuid.UUID
	ExternalID string
	Message    Message
	Status     Status
}

func (m MessageBody) GetID() uuid.UUID {
	return m.id
}

type Response struct {
	MessageBody
}

type Request struct {
	MessageBody
	response chan Response
}

func NewRequest(externalID string) *Request {
	req := new(Request)
	req.ExternalID = externalID
	req.Status = StatusNew

	req.response = make(chan Response, responseBufferSize)

	req.id = uuid.New()

	return req
}

func NewRequestWith(externalID string, reschan chan Response) *Request {
	req := new(Request)
	req.ExternalID = externalID
	req.Status = StatusNew

	req.response = reschan

	req.id = uuid.New()

	return req
}

func NewResponse(requestid uuid.UUID, externalID string) *Response {
	res := new(Response)
	res.ExternalID = externalID
	res.Status = StatusNew

	res.id = requestid
	return res
}

func (req *Request) Receive() Response {
	return <-req.response
}

func (req *Request) Respond(res Response) {
	req.response <- res
}

func (req *Request) SetResponder(reschan chan Response) {
	req.response = reschan
}

func (req *Request) GetResponder() chan<- Response {
	return req.response
}

func (req *Request) GetReceiver() <-chan Response {
	return req.response
}

type BuildInfo struct {
	Service struct {
		Name     string `json:"name"`
		Version  string `json:"version"`
		CommitId string `json:"commitId"`
	} `json:"service"`
	Components []struct {
		Name     string `json:"name"`
		Version  string `json:"version"`
		CommitId string `json:"commitId"`
	} `json:"components"`
}
