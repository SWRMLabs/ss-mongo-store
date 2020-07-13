package mongo

import (
	"os"
	"testing"
	"time"

	"github.com/StreamSpace/ss-store"
	"github.com/google/uuid"
	logger "github.com/ipfs/go-log/v2"
)

type successStruct struct {
	Namespace string
	Id        string `bson:"_id",json:"_id"`
	Status    string `status,omitempty`
	Created   int64  `created,omitempty`
	Updated   int64  `updated,omitempty`
}

func (t *successStruct) GetNamespace() string { return t.Namespace }

func (t *successStruct) GetId() string { return t.Id }

func (t *successStruct) SetCreated(unixTime int64) { t.Created = unixTime }

func (t *successStruct) SetUpdated(unixTime int64) { t.Updated = unixTime }

func (t *successStruct) GetCreated() int64 { return t.Created }

func (t *successStruct) GetUpdated() int64 { return t.Updated }

var mngoCnfg MongoConfig

var mngoHndlr *ssMongoHandler

var Id string

func TestMain(m *testing.M) {
	mngoCnfg = MongoConfig{
		DbName: "ssMongo",
		Host:   "mongodb://localhost:27017",
	}
	Id = uuid.New().String()
	logger.SetLogLevel("*", "Debug")
	code := m.Run()
	os.Exit(code)
}

func TestMongoHander(t *testing.T) {
	handler := mngoCnfg.Handler()

	if handler != "mongodb" {
		t.Fatalf("Handler returned %s", handler)
	}
}

func TestNewMongoStore(t *testing.T) {
	_, err := NewMongoStore(&mngoCnfg)
	if err != nil {
		t.Fatalf("Mongo store init failed")
	}
}

func TestNewMongoCreate(t *testing.T) {
	mngoHndlr, err := NewMongoStore(&mngoCnfg)
	if err != nil {
		t.Fatalf("Mongo store init failed")
	}
	d := successStruct{
		Namespace: "StreamSpace",
		Id:        Id,
		Status:    "Golang create",
		Created:   time.Now().Unix(),
		Updated:   time.Now().Unix(),
	}
	err = mngoHndlr.Create(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestNewMongoRead(t *testing.T) {
	mngoHndlr, err := NewMongoStore(&mngoCnfg)
	if err != nil {
		t.Fatalf("Mongo store init failed")
	}
	d := successStruct{
		Namespace: "StreamSpace",
		Id:        Id,
	}
	err = mngoHndlr.Read(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestNewMongoUpdate(t *testing.T) {
	mngoHndlr, err := NewMongoStore(&mngoCnfg)
	if err != nil {
		t.Fatalf("Mongo store init failed")
	}

	d := successStruct{
		Namespace: "StreamSpace",
		Id:        Id,
		Status:    "Golang update",
		Updated:   time.Now().Unix(),
	}
	err = mngoHndlr.Update(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestNewMongoDelete(t *testing.T) {
	mngoHndlr, err := NewMongoStore(&mngoCnfg)
	if err != nil {
		t.Fatalf("Mongo store init failed")
	}
	d := successStruct{
		Namespace: "StreamSpace",
		Id:        Id,
	}
	err = mngoHndlr.Delete(&d)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestNewMongoNaturalList(t *testing.T) {
	mngoHndlr, err := NewMongoStore(&mngoCnfg)
	if err != nil {
		t.Fatalf("Mongo store init failed")
	}

	var sort store.Sort

	sort = 0

	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	arr := store.Items{}

	for i := 0; int64(i) < opts.Limit; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
		}
		arr = append(arr, &d)
	}

	_, err = mngoHndlr.List(arr, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	log.Debug(arr)
}

func TestNewMongoCreatedAscList(t *testing.T) {
	mngoHndlr, err := NewMongoStore(&mngoCnfg)
	if err != nil {
		t.Fatalf("Mongo store init failed")
	}

	var sort store.Sort

	sort = 1

	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	arr := store.Items{}

	for i := 0; int64(i) < opts.Limit; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
		}
		arr = append(arr, &d)
	}

	_, err = mngoHndlr.List(arr, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	log.Debug(arr)
}

func TestNewMongoCreatedDscList(t *testing.T) {
	mngoHndlr, err := NewMongoStore(&mngoCnfg)
	if err != nil {
		t.Fatalf("Mongo store init failed")
	}

	var sort store.Sort

	sort = 2

	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	arr := store.Items{}

	for i := 0; int64(i) < opts.Limit; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
		}
		arr = append(arr, &d)
	}

	_, err = mngoHndlr.List(arr, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	log.Debug(arr)
}

func TestNewMongoUpdatedAscList(t *testing.T) {
	mngoHndlr, err := NewMongoStore(&mngoCnfg)
	if err != nil {
		t.Fatalf("Mongo store init failed")
	}

	var sort store.Sort

	sort = 3

	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	arr := store.Items{}

	for i := 0; int64(i) < opts.Limit; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
		}
		arr = append(arr, &d)
	}

	_, err = mngoHndlr.List(arr, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	log.Debug(arr)
}

func TestNewMongoUpdatedDscList(t *testing.T) {
	mngoHndlr, err := NewMongoStore(&mngoCnfg)
	if err != nil {
		t.Fatalf("Mongo store init failed")
	}

	var sort store.Sort

	sort = 4

	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Sort:  sort,
	}

	arr := store.Items{}

	for i := 0; int64(i) < opts.Limit; i++ {
		d := successStruct{
			Namespace: "StreamSpace",
		}
		arr = append(arr, &d)
	}

	_, err = mngoHndlr.List(arr, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	log.Debug(arr)
}
