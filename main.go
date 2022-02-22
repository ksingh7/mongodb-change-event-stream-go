package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DbEvent struct {
	DocumentKey   documentKey `bson:"documentKey"`
	OperationType string      `bson:"operationType"`
}
type documentKey struct {
	ID primitive.ObjectID `bson:"_id"`
}
type result struct {
	ID         primitive.ObjectID `bson:"_id"`
	UserID     string             `bson:"userID"`
	DeviceType string             `bson:"deviceType"`
	GameState  string             `bson:"gameState"`
}

func listenToDBChangeStream(
	routineCtx context.Context,
	waitGroup sync.WaitGroup,
	stream *mongo.ChangeStream,
	collection *mongo.Collection,
) {
	// Cleanup defer functions when this function exits
	defer stream.Close(routineCtx)
	// Wrap the worker call in a closure that makes sure to tell the WaitGroup that this worker is done
	defer waitGroup.Done()

	// Whenever there is a change in the bike-factory collection, decode the change
	for stream.Next(routineCtx) {
		var DbEvent DbEvent
		if err := stream.Decode(&DbEvent); err != nil {
			panic(err)
		}
		if DbEvent.OperationType == "insert" {
			fmt.Println("Insert operation detected")
		} else if DbEvent.OperationType == "update" {
			fmt.Println("Update operation detected")
		} else if DbEvent.OperationType == "delete" {
			fmt.Println("Delete operation detected : Unable to pull changes as its record is deleted")
		}

		// Print out the document that was inserted or updated
		if DbEvent.OperationType == "insert" || DbEvent.OperationType == "update" {
			// Find the mongodb document based on the objectID
			var result result
			err := collection.FindOne(context.TODO(), DbEvent.DocumentKey).Decode(&result)
			if err != nil {
				log.Fatal(err)
			}
			// Convert changd MongoDB document from BSON to JSON
			data, writeErr := bson.MarshalExtJSON(result, false, false)
			if writeErr != nil {
				log.Fatal(writeErr)
			}
			// Print the changed document in JSON format
			fmt.Println(string(data))
			fmt.Println("")
		}
	}
}

func main() {
	// waitGroup to wait for all goroutines launched here to finish
	var waitGroup sync.WaitGroup

	// Set client options and connect to MongoDB
	client, err := mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(os.Getenv("MONGODB_URI")),
	)
	if err != nil {
		panic(err)
	}
	// Cleanup the connection when main function exists
	defer client.Disconnect(context.TODO())

	// set Mongodb database and collection name
	database := client.Database("change-stream-demo")
	collection := database.Collection("bike-factory")

	/* Create a change stream to listen to changes in the bike-factory collection
	   This will watch all any and all changes to the documents within the collection
	   and will be later used to iterate over indefinately */
	stream, err := collection.Watch(context.TODO(), mongo.Pipeline{})
	if err != nil {
		panic(err)
	}

	// Waitgroup counter
	waitGroup.Add(1)

	routineCtx, cancelFn := context.WithCancel(context.Background())
	_ = cancelFn

	/* Watches bike-factory collection in summit-demo database and prints out any changed document
	   go-routine to make code non-blocking */
	go listenToDBChangeStream(routineCtx, waitGroup, stream, collection)

	// Insert a MongoDB record every 5 seconds
	go insertRecord(collection)

	// Block until the WaitGroup counter goes back to 0; all the workers notified they’re done.
	waitGroup.Wait()
}

// function to insert data records to MongoDB collection
func insertRecord(collection *mongo.Collection) {
	// pre-populated values for DeviceType and GameState
	DeviceType := make([]string, 0)
	DeviceType = append(
		DeviceType,
		"mobile",
		"laptop",
		"karan-board",
		"tablet",
		"desktop",
		"smart-watch",
	)
	GameState := make([]string, 0)
	GameState = append(GameState, "playing", "paused", "stopped", "finished", "failed")

	// insert new records to MongoDB every 5 seconds
	for {
		item := result{
			ID:         primitive.NewObjectID(),
			UserID:     strconv.Itoa(rand.Intn(10000)),
			DeviceType: DeviceType[rand.Intn(len(DeviceType))],
			GameState:  GameState[rand.Intn(len(GameState))],
		}
		_, err := collection.InsertOne(context.TODO(), item)
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(5 * time.Second)
	}
}
