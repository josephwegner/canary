package main

import (
	"fmt"
	"net/http"
	"crypto/md5"
	"io"
	"encoding/hex"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)


/** Define Asset Type **/
type Asset struct {
	Id bson.ObjectId `bson:"_id"`
	Url string
	Checksum string
}

func (a *Asset) describe() {
	fmt.Printf("Asset's URL is %s\n", a.Url)
}

func (a *Asset) confirmChecksum(coll *mgo.Collection, finished chan int) {
	defer func() { finished <- 1 }()

	fmt.Printf("Checking %s\n", a.Url)

	activeRequests <- 1
	resp, err := http.Get(a.Url)
	<- activeRequests

	if err != nil {
		fmt.Printf("Got HTTP Error for %s.  Error: %v\n", a.Url, err)
		return
	}

	newHasher := md5.New()
	io.Copy(newHasher, resp.Body)
	newHash := hex.EncodeToString(newHasher.Sum(nil))

	if newHash != a.Checksum {
		fmt.Printf("Hashes did not match: %s != %s\n", newHash, a.Checksum)
		a.Checksum = newHash
		err = coll.Update(bson.M{"_id": a.Id}, a)
		if err != nil {
			fmt.Printf("Error updating hash, got error %v\n", err)
		}
	} else {
		fmt.Printf("Hashes Matched!\n")
	}

}

/** Global Variables **/

//This will hold an inconsequential int for each active HTTP request.
//The point is that we should only have 5 concurrent HTTP requests running
var activeRequests = make(chan int, 5) 

/** Main **/
func main() {

	mongoSess, err := connectToMongo("mongodb://localhost/canarydb")
	defer mongoSess.Close()

	dbAssets := mongoSess.DB("canarydb").C("assets")

	assets, err := getAllAssets(dbAssets)
	if err != nil {
		fmt.Printf("Error getting assets from db %v\n", err)
		return
	}

	runningChecks := make(chan int, len(assets))
	for i := range assets {
		go assets[i].confirmChecksum(dbAssets, runningChecks)
	}

	//This is just so that main() stays open until all of the goroutines finish
	//At this point, runningChecks doesn't actually contain anything
	for i := 0; i < len(assets); i++ {
		<- runningChecks
	}
}

/** Sync Functions **/

func connectToMongo(uri string) (*mgo.Session, error) {
	sess, err := mgo.Dial(uri)
	sess.SetSafe(&mgo.Safe{})

	return sess, err
}

func getAllAssets(coll *mgo.Collection) ([]Asset, error) {
	assets := []Asset{}
	err := coll.Find(nil).All(&assets)

	return assets, err
}