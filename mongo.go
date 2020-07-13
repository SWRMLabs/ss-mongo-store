package mongo

import (
	"context"
	"time"

	"github.com/StreamSpace/ss-store"
	"github.com/google/uuid"
	logger "github.com/ipfs/go-log/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var log = logger.Logger("store/mongo")

type ssMongoHandler struct {
	dbP  *mongo.Database
	conf *MongoConfig
}

type MongoConfig struct {
	DbName string
	Host   string // Will concatenate username, password and URI in Host
}

func (b *MongoConfig) Handler() string {
	return "mongodb"
}

func NewMongoStore(mongoConf *MongoConfig) (store.Store, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoConf.Host))
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		log.Errorf("Client is not connected %s", err.Error())
		return nil, err
	}
	return &ssMongoHandler{
		dbP:  client.Database(mongoConf.DbName),
		conf: mongoConf,
	}, nil
}

// Create a mongoDB record
func (m *ssMongoHandler) Create(i store.Item) error {
	idSetter, ok := i.(store.IDSetter)
	if ok == true {
		idSetter.SetID(uuid.New().String())
	}
	if timeTracker, ok := i.(store.TimeTracker); ok {
		var unixTime = time.Now().Unix()
		timeTracker.SetCreated(unixTime)
		timeTracker.SetUpdated(unixTime)
	}
	collection := m.dbP.Collection(i.GetNamespace())
	insertResult, err := collection.InsertOne(context.Background(), i)
	if err != nil {
		log.Errorf("No insertion %s", err.Error())
		return err
	}
	log.Debug("Inserted a single document: ", insertResult.InsertedID)
	return nil
}

// Read a mongoDB record
func (m *ssMongoHandler) Read(i store.Item) error {
	collection := m.dbP.Collection(i.GetNamespace())
	filter := bson.D{{"_id", i.GetId()}}
	err := collection.FindOne(context.Background(), filter).Decode(i)
	if err != nil {
		log.Errorf("The filter did not match with any documents in the collection %s", err.Error())
		return err
	}
	log.Debugf("Found document %v", i.GetNamespace())
	return nil
}

// Update a mongoDB record
func (m *ssMongoHandler) Update(i store.Item) error {
	if timeTracker, ok := i.(store.TimeTracker); ok {
		timeTracker.SetUpdated(time.Now().Unix())
	}

	collection := m.dbP.Collection(i.GetNamespace())
	filter := bson.D{{"_id", i.GetId()}}
	updateResult, err := collection.UpdateOne(context.Background(), filter, bson.D{{"$set", i}})
	if err != nil {
		log.Errorf("No updation %s", err.Error())
		return err
	}

	if updateResult.MatchedCount != 1 && updateResult.ModifiedCount != 1 {
		log.Debugf("No updation in collection, Matched %v document and updated %v document.\n", updateResult.MatchedCount, updateResult.ModifiedCount)
		return err
	}
	return nil
}

// Delete a mongoDB record
func (m *ssMongoHandler) Delete(i store.Item) error {
	collection := m.dbP.Collection(i.GetNamespace())
	filter := bson.D{{"_id", i.GetId()}}
	deleteResult, err := collection.DeleteOne(context.Background(), filter)
	if err != nil {
		log.Errorf("No deletion %s", err.Error())
		return err
	}
	if deleteResult.DeletedCount != 1 {
		log.Debugf("Nothing is deleted, Deleted %v document\n", deleteResult.DeletedCount)
		return err
	}
	return nil
}

//List dislplays all record in a mongoDB collection
func (m *ssMongoHandler) List(i store.Items, o store.ListOpt) (int, error) {
	var (
		collection  = m.dbP.Collection(i[0].GetNamespace())
		skip        = (o.Page) * (o.Limit)
		listCounter = 0
	)
	order := o.Sort
	ctx := context.Background()
	opts := options.FindOptions{
		Skip:  &skip,
		Limit: &o.Limit,
	}

	switch order {
	case 0: // Natural order
		cursor, err := collection.Find(ctx, bson.D{}, &opts)
		if err != nil {
			log.Errorf("Not able to query data %s", err.Error())
			return 0, err
		}

		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			err = cursor.Decode(i[listCounter])
			if err != nil {
				log.Errorf("Failed decoding Err:%s", err.Error())
				return 0, err
			}
			listCounter++
		}
	case 1: // SortCreatedAsc : Oldest to newest
		opts.Sort = bson.D{{"created", 1}}
		cursor, err := collection.Find(ctx, bson.D{}, &opts)
		if err != nil {
			log.Errorf("Not able to query data %s", err.Error())
			return 0, err
		}

		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			err = cursor.Decode(i[listCounter])
			if err != nil {
				log.Errorf("Failed decoding Err:%s", err.Error())
				return 0, err
			}
			listCounter++
		}
	case 2: // SortCreatedDsc : Newest to oldest
		opts.Sort = bson.D{{"created", -1}}
		cursor, err := collection.Find(ctx, bson.D{}, &opts)
		if err != nil {
			log.Errorf("Not able to query data %s", err.Error())
			return 0, err
		}

		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			err = cursor.Decode(i[listCounter])
			if err != nil {
				log.Errorf("Failed decoding Err:%s", err.Error())
				return 0, err
			}
			listCounter++
		}
	case 3: // SortUpdatedAsc : Update oldest to newest
		opts.Sort = bson.D{{"updated", 1}}
		cursor, err := collection.Find(ctx, bson.D{}, &opts)
		if err != nil {
			log.Errorf("Not able to query data %s", err.Error())
			return 0, err
		}

		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			err = cursor.Decode(i[listCounter])
			if err != nil {
				log.Errorf("Failed decoding Err:%s", err.Error())
				return 0, err
			}
			listCounter++
		}
	case 4: // SortUpdatedDsc : Update newest to oldest
		opts.Sort = bson.D{{"updated", -1}}
		cursor, err := collection.Find(ctx, bson.D{}, &opts)
		if err != nil {
			log.Errorf("Not able to query data %s", err.Error())
			return 0, err
		}

		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			err = cursor.Decode(i[listCounter])
			if err != nil {
				log.Errorf("Failed decoding Err:%s", err.Error())
				return 0, err
			}
			listCounter++
		}
	}
	return listCounter, nil
}

func (m *ssMongoHandler) Close() error {
	if m.dbP != nil {
		return m.dbP.Client().Disconnect(context.Background())
	}
	return nil
}
