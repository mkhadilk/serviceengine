package mongoadaptor

import (
	"bytes"
	"context"
	"log"
	"testing"
	"time"

	"github.com/google/uuid"

	"go.mongodb.org/mongo-driver/mongo/options"

	se "github.com/mkhadilk/serviceengine"
	"go.mongodb.org/mongo-driver/mongo"
)

func getCollection(name string) *mongo.Collection {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017/?directConnection=true"))
	if err != nil {
		log.Printf("Error %+v", err)
		return nil
	}

	// Connect to the MongoDB server
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		log.Printf("Error %+v", err)
		return nil
	}

	// Create a MongoDB collection

	MONGODBNAME := "test"

	db := client.Database(MONGODBNAME)

	err = db.Client().Ping(nil, nil)

	if err != nil {
		log.Printf("error %+v", err)
		return nil
	}

	return db.Collection(name)
}

func TestMongoMessageInsert(t *testing.T) {
	col := getCollection("notifications")
	if col == nil {
		log.Println("Save failed; can not get collection")
		t.FailNow()
	}
	re := se.NewRequest("extid")
	body := struct{ Data string }{
		Data: "testdata",
	}
	re.AsJSON(body)
	m := NewMongoRequest(*re)
	m.Status = MongoStatusNew

	log.Printf("Saving %+v", *m)

	err := m.Save(col)
	if err != nil {
		log.Printf("Save failed %+v", err)
		col.Database().Client().Disconnect(nil)
		t.FailNow()
	}

	col.Drop(nil)
}

func TestMongoMessageFetchSelective(t *testing.T) {

	col := getCollection("notifications")
	if col == nil {
		log.Println("Save failed; can not get collection")
		t.FailNow()
	}
	re := se.NewRequest("extid")
	body := struct{ Data string }{
		Data: "testdata",
	}
	re.AsJSON(body)
	m := NewMongoRequest(*re)
	m.Status = MongoStatusNew

	err := m.Update(col)

	if err != nil {
		log.Printf("Insert/Update failed %+v", err)
		col.Database().Client().Disconnect(nil)
		t.FailNow()
	}

	log.Printf("Fetching %+v", *m)

	reqs, err := Fetch(col, m.ID, "", "", MongoStatusAny)
	if err != nil {
		log.Printf("Fetch failed %+v", err)
		col.Database().Client().Disconnect(nil)
		t.FailNow()
	}

	log.Printf("Records found %+v", reqs)

	if len(reqs) != 1 || reqs[0].ID != m.ID || reqs[0].ExternalID != m.ExternalID || bytes.Compare(reqs[0].Request.Message, m.Request.Message) != 0 {
		log.Printf("Fetch failed %+v is not same as %+v", reqs, *m)
		t.FailNow()
	}

	col.Drop(nil)
}
func TestMongoMessageDeleteSelective(t *testing.T) {

	col := getCollection("notifications")
	if col == nil {
		log.Println("Save failed; can not get collection")
		t.FailNow()
	}
	re := se.NewRequest("extid")
	body := struct{ Data string }{
		Data: "testdata",
	}
	re.AsJSON(body)
	m := NewMongoRequest(*re)
	m.Status = MongoStatusNew

	err := m.Update(col)

	if err != nil {
		log.Printf("Insert/Update failed %+v", err)
		col.Database().Client().Disconnect(nil)
		t.FailNow()
	}

	log.Printf("Deleting %+v", *m)

	err = m.Delete(col)

	log.Printf("Delete error %+v", err)

	reqs, err := Fetch(col, m.ID, "", "", MongoStatusAny)
	if err != nil {
		log.Printf("Fetch failed %+v", err)
		col.Database().Client().Disconnect(nil)
		t.FailNow()
	}

	log.Printf("Records found %+v", reqs)

	if len(reqs) != 0 {
		log.Printf("Delete failed; expected 0 but found %d", len(reqs))
		t.FailNow()
	}

	col.Drop(nil)
}
func TestMongoMessageFetchAll(t *testing.T) {
	col := getCollection("notifications")
	if col == nil {
		log.Println("Save failed; can not get collection")
		t.FailNow()
	}
	re := se.NewRequest("extid")
	body := struct{ Data string }{
		Data: "testdata",
	}
	re.AsJSON(body)
	m := NewMongoRequest(*re)
	m.Status = MongoStatusNew

	err := m.Update(col)

	if err != nil {
		log.Printf("Insert/Update failed %+v", err)
		col.Database().Client().Disconnect(nil)
		t.FailNow()
	}

	log.Printf("Fetching %+v", *m)

	reqs, err := Fetch(col, "", "", "", MongoStatusAny)
	if err != nil {
		log.Printf("Fetch failed %+v", err)
		col.Database().Client().Disconnect(nil)
		t.FailNow()
	}

	log.Printf("Records found %+v", reqs)

	if len(reqs) < 1 {
		log.Printf("Fetch failed; records found %d; execting at least 1", len(reqs))
	}

	col.Drop(nil)
}

func TestMongoPublicSubscribe(t *testing.T) {
	topic := "serviceproviders"
	se.ENGINE.AddConsumer(topic, *NewMongoConsumer(topic, getCollection(topic).Database()))

	re := se.NewRequest(uuid.New().String())
	body := struct{ Data string }{
		Data: "SP message",
	}
	re.AsJSON(body)

	log.Printf("Message with ID %s Message\n%+v", re.GetID().String(), *re)

	se.ENGINE.Execute(topic, *re)
	res := re.Receive()

	log.Printf("Received %+v", res)

	lst, err := Fetch(getCollection(topic), re.GetID().String(), "", "", MongoStatusAny)

	if err != nil {
		log.Printf("Error %+v", err)
		t.Fail()
	}

	log.Printf("returned requests %+v", lst)

	if len(lst) != 1 {
		log.Printf("Error %+v", err)
		t.FailNow()
	}

	if lst[0].ID != re.GetID().String() {
		log.Printf("Error wrong message ID %s expected; actual %s", re.GetID().String(), lst[0].ID)
		t.FailNow()
	}

	var body2 struct{ Data string }

	lst[0].Request.FromJSONInto(&body2)

	log.Printf("Notification \nSent %+v\nReceived %+v", body, body2)

	if body.Data != body2.Data {
		log.Printf("Error wrong message Data for ID %s; expected %s; actual %s", re.GetID().String(), body.Data, body2.Data)
		t.FailNow()
	}

	getCollection(topic).Drop(nil)

}
