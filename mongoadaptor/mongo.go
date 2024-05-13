package mongoadaptor

import (
	"context"
	"errors"
	"log"

	"github.com/google/uuid"

	"go.mongodb.org/mongo-driver/mongo/options"

	se "github.com/mkhadilk/serviceengine"
	"go.mongodb.org/mongo-driver/bson"

	"go.mongodb.org/mongo-driver/mongo"
)

type MongoStatus string

const (
	MongoStatusAny       MongoStatus = ""
	MongoStatusNew       MongoStatus = "new"
	MongoStatusProcessed MongoStatus = "processed"
)

type MongoRequest struct {
	ID         string      `json:"id" bson:"_id"` // Private field for ID
	ExternalID string      `json:"externalid" bson:"externalid"`
	Request    se.Request  `json:"request"`
	Topic      string      `json:"topic" bson:"topic"` //optional
	Status     MongoStatus `json:"mongostatus" bson:"mongostatus"`
}

func NewMongoRequest(req se.Request) *MongoRequest {
	me := new(MongoRequest)
	me.ID = req.GetID().String()
	me.ExternalID = req.ExternalID
	me.Request = req
	me.Status = MongoStatusNew
	return me
}

// This will insert only.
func (m *MongoRequest) Save(cl *mongo.Collection) error {
	if cl == nil {
		return errors.New("Collection is not initilized...")
	}
	io, err := cl.InsertOne(nil, m)
	if err != nil {
		return err
	}
	if io.InsertedID != nil {
		if err == nil {
			return nil
		} else {
			return err
		}
	} else {
		return errors.Join(errors.New("Can not save!"), err)
	}
}

func (m *MongoRequest) Delete(cl *mongo.Collection) error {
	if cl == nil {
		return errors.New("Collection is not initilized...")
	}
	filter := bson.M{"_id": m.ID}
	io, err := cl.DeleteOne(context.TODO(), filter)
	if err != nil {
		return err
	}
	if io.DeletedCount == 1 {
		return nil
	} else {
		log.Printf("Deleted count %d is not equal to 1", io.DeletedCount)
		return errors.Join(errors.New("Not Deleted!"), err)
	}
}

// This will update or insert based on if the id exists or not
func (m *MongoRequest) Update(cl *mongo.Collection) error {
	u := new(options.UpdateOptions)
	update := bson.M{"$set": *m}
	ur, err := cl.UpdateByID(nil, m.ID, update, u.SetUpsert(true))
	if err != nil || (ur.UpsertedCount == 0 && ur.MatchedCount == 0 && ur.ModifiedCount == 0) {
		log.Printf("Upsert failed %+v upsert %+v", err, ur)
		return errors.New("Can not insert or save!")
	}
	return nil
}

// Pass any string as "" for id, externalid, topic to be ignored for filter and status as MongoStatusAny to ignore
func Fetch(cl *mongo.Collection, id, externalid, topic string, status MongoStatus) ([]MongoRequest, error) {

	var all []MongoRequest
	all = make([]MongoRequest, 0)
	filter := bson.M{}
	if id != "" {
		filter["_id"] = id
	}
	if externalid != "" {
		filter["externalid"] = externalid
	}
	if topic != "" {
		filter["topic"] = topic
	}

	if status != MongoStatusAny {
		filter["mongostatus"] = status
	}
	var cur *mongo.Cursor
	var err error
	if len(filter) == 0 {
		log.Println("Fetch all")
		filter := bson.D{}
		cur, err = cl.Find(context.TODO(), filter)
	} else {
		cur, err = cl.Find(context.TODO(), filter)
	}
	if err != nil {
		return all, err
	}

	for cur.TryNext(context.Background()) {
		var rec MongoRequest
		err = cur.Decode(&rec)
		if err == nil {
			all = append(all, rec)
		}
	}

	return all, err
}

func NewMongoConsumer(topic string, db *mongo.Database) *se.Consumer {
	col := db.Collection(topic)
	return se.NewConsumer(func(req se.Request) {
		mr := NewMongoRequest(req)
		mr.Topic = topic
		mr.Status = MongoStatusNew
		err := mr.Save(col)
		res := se.NewResponse(uuid.MustParse(mr.ID), mr.ExternalID)
		if err == nil {
			res.Status = se.StatusError
		} else {
			res.Status = se.StatusReceived
		}
		req.Respond(*res)
	})
}
