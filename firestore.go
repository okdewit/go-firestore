package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/okdewit/go-utils/catch"
	"google.golang.org/api/option"
	"sync"
	"time"
)

var ctx context.Context
var Authfile string
var client *firestore.Client
var collectionRef *firestore.CollectionRef

func Init(project string) (err error) {
	ctx = context.Background()
	auth := option.WithCredentialsFile(Authfile)

	// Try to create a client with auth file
	client, err = firestore.NewClient(ctx, project, auth)
	if err != nil {
		client, err = firestore.NewClient(ctx, project)
	}
	if err != nil { return }
	return
}

func SetCollection(name string) {
	collectionRef = client.Collection(name)
}

func GetBetween(from time.Time, until time.Time) chan firestore.DocumentSnapshot {

	fmt.Printf("%v\n", from.In(time.UTC).Format(time.RFC3339))

	docs, err := collectionRef.
		Where("CreatedAt", ">=", from.In(time.UTC).Format(time.RFC3339)).
		Where("CreatedAt", "<=", until.In(time.UTC).Format(time.RFC3339)).
		Documents(ctx).GetAll()

	catch.Check(err)

	docChannel := make(chan firestore.DocumentSnapshot, len(docs))
	defer close(docChannel)

	wg := sync.WaitGroup{}
	wg.Add(len(docs))
	for i, doc := range docs {
		go func (i int, doc *firestore.DocumentSnapshot) {
			defer wg.Done()
			docChannel <- *doc
		} (i, doc)
	}
	wg.Wait()

	return docChannel
}

func WriteDoc(name string, data interface{}) *firestore.WriteResult {
	result, err := collectionRef.Doc(name).Set(ctx, data)
	catch.Check(err)
	return result
}





