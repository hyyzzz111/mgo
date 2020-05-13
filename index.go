package mgo

import (
	"github.com/yaziming/mgo/bson"
	"time"
	
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Index struct {
	Key           []string // Index key fields; prefix name with dash (-) for descending order
	Unique        bool     // Prevent two documents from having the same index key
	DropDups      bool     // Drop documents with the same index key as a previously indexed one
	Background    bool     // Build index in background and return immediately
	Sparse        bool     // Only index documents containing the Key fields
	PartialFilter bson.M   // Partial index filter expression
	
	// If ExpireAfter is defined the server will periodically delete
	// documents with indexed time.Time older than the provided delta.
	ExpireAfter time.Duration
	
	// Name holds the stored index name. On creation if this field is unset it is
	// computed by EnsureIndex based on the index key.
	Name string
	
	// Properties for spatial indexes.
	//
	// Min and Max were improperly typed as int when they should have been
	// floats.  To preserve backwards compatibility they are still typed as
	// int and the following two fields enable reading and writing the same
	// fields as float numbers. In mgo.v3, these fields will be dropped and
	// Min/Max will become floats.
	Min, Max   int
	Minf, Maxf float64
	BucketSize float64
	Bits       int
	
	// Properties for text indexes.
	DefaultLanguage  string
	LanguageOverride string
	
	// Weights defines the significance of provided fields relative to other
	// fields in a text index. The score for a given word in a document is derived
	// from the weighted sum of the frequency for each of the indexed fields in
	// that document. The default field weight is 1.
	Weights map[string]int
	
	// Collation defines the collation to use for the index.
	Collation *Collation
}

func (i *Index) ToIndexModels() mongo.IndexModel {
	indexOpts := options.Index().
		SetBackground(i.Background).
		SetBits(int32(i.Bits)).
		SetBucketSize(int32(i.BucketSize)).
		SetDefaultLanguage(i.DefaultLanguage).
		SetExpireAfterSeconds(int32(i.ExpireAfter)).
		SetLanguageOverride(i.LanguageOverride).
		SetMax(float64(i.Max)).
		SetMin(i.Minf).
		SetName(i.Name).
		SetPartialFilterExpression(i.PartialFilter).
		SetSparse(i.Sparse).
		SetWeights(i.Weights).
		SetUnique(i.Unique)
	return mongo.IndexModel{
		Keys:    i.Key,
		Options: indexOpts,
	}
}
