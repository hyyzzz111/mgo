package mgo

import (
	"context"
	"errors"
	"mgo/bson"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	ErrIndexNotFound = errors.New("index not found")
)

type Index struct {
	Key    []string // Index key fields; prefix name with dash (-) for descending order
	Unique bool     // Prevent two documents from having the same index key

	//DropDups      bool     // deprecated since version 2.7.5 DropCollection documents with the same index key as a previously indexed one
	Background    bool   // Build index in background and return immediately
	Sparse        bool   // Only index documents containing the Key fields
	PartialFilter bson.M // Partial index filter expression

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

func (i *Index) ToIndexModels() (model mongo.IndexModel, err error) {
	indexOpts := options.Index().
		SetBackground(i.Background)
	if i.Bits > 0 {
		indexOpts.SetBits(int32(i.Bits))
	}
	if i.Sparse {
		indexOpts.SetSparse(i.Sparse)
	}
	if i.BucketSize > 0 {
		indexOpts.SetBucketSize(int32(i.BucketSize))
	}
	if len(i.PartialFilter) > 0 {
		indexOpts.SetPartialFilterExpression(i.PartialFilter)
	}
	if i.Maxf != i.Minf {
		indexOpts.SetMax(i.Maxf)
		indexOpts.SetMin(i.Minf)

	}

	if i.Unique {
		indexOpts.SetUnique(i.Unique)
	}
	if i.Minf > 0 {
		indexOpts.SetMin(i.Minf)
	}
	if i.ExpireAfter.Seconds() > 0 {
		indexOpts.SetExpireAfterSeconds(int32(i.ExpireAfter.Seconds()))
	}
	if i.LanguageOverride != "" {
		indexOpts.SetLanguageOverride(i.LanguageOverride)
	}
	if i.DefaultLanguage != "" {
		indexOpts.SetDefaultLanguage(i.DefaultLanguage)
	}

	info, err := parseIndexKey(i.Key)
	if err != nil {
		return
	}
	weights := map[string]interface{}{}
	if len(info.weights) > 0 {
		for _, v := range info.weights {
			weights[v.Key] = v.Value
		}
	}
	if len(i.Weights) > 0 {
		for k, v := range i.Weights {
			weights[k] = v
		}
	}
	if len(weights) > 0 {
		indexOpts.SetWeights(weights)
	}

	if i.Name != "" {
		indexOpts.SetName(i.Name)
	} else {
		indexOpts.SetName(info.name)
	}
	return mongo.IndexModel{
		Keys:    info.key,
		Options: indexOpts,
	}, nil
}
func simpleIndexKey(realKey bson.D) (key []string) {
	for i := range realKey {
		var vi int
		field := realKey[i].Key

		switch realKey[i].Value.(type) {
		case int64:
			vf, _ := realKey[i].Value.(int64)
			vi = int(vf)
		case int32:
			vf, _ := realKey[i].Value.(int32)
			vi = int(vf)
		case float64:
			vf, _ := realKey[i].Value.(float64)
			vi = int(vf)
		case float32:
			vf, _ := realKey[i].Value.(float32)
			vi = int(vf)
		case string:
			if vs, ok := realKey[i].Value.(string); ok {
				key = append(key, "$"+vs+":"+field)
				continue
			}
		case int:
			vi = realKey[i].Value.(int)
		}

		if vi == 1 {
			key = append(key, field)
			continue
		}
		if vi == -1 {
			key = append(key, "-"+field)
			continue
		}
		panic("Got unknown index key type for field " + field)
	}
	return
}

type indexSpec struct {
	Name, NS                string
	Key                     bson.D
	Unique                  bool    `bson:",omitempty"`
	DropDups                bool    `bson:"dropDups,omitempty"`
	Background              bool    `bson:",omitempty"`
	Sparse                  bool    `bson:",omitempty"`
	Bits                    int     `bson:",omitempty"`
	Min                     float64 `bson:",omitempty"`
	Max                     float64 `bson:",omitempty"`
	BucketSize              float64 `bson:"bucketSize,omitempty"`
	ExpireAfter             int     `bson:"expireAfterSeconds,omitempty"`
	Weights                 bson.D  `bson:",omitempty"`
	DefaultLanguage         string  `bson:"default_language,omitempty"`
	LanguageOverride        string  `bson:"language_override,omitempty"`
	TextIndexVersion        int     `bson:"textIndexVersion,omitempty"`
	PartialFilterExpression bson.M  `bson:"partialFilterExpression,omitempty"`

	Collation *Collation `bson:"collation,omitempty"`
}

func (c *Collection) Indexes() (indexes []Index, err error) {
	ctx := context.Background()
	cursor, err := c.collection.Indexes().List(ctx)
	if err != nil {
		return
	}
	if err = cursor.Err(); err != nil {
		return
	}
	for cursor.Next(ctx) {
		var current indexSpec
		_ = cursor.Decode(&current)
		indexes = append(indexes, indexFromSpec(current))
	}
	sort.Sort(indexSlice(indexes))

	return
}
func indexFromSpec(spec indexSpec) Index {
	index := Index{
		Name:             spec.Name,
		Key:              simpleIndexKey(spec.Key),
		Unique:           spec.Unique,
		Background:       spec.Background,
		Sparse:           spec.Sparse,
		Minf:             spec.Min,
		Maxf:             spec.Max,
		Bits:             spec.Bits,
		BucketSize:       spec.BucketSize,
		DefaultLanguage:  spec.DefaultLanguage,
		LanguageOverride: spec.LanguageOverride,
		ExpireAfter:      time.Duration(spec.ExpireAfter) * time.Second,
		Collation:        spec.Collation,
		PartialFilter:    spec.PartialFilterExpression,
	}
	//if float64(int(spec.Min)) == spec.Min && float64(int(spec.Max)) == spec.Max {
	//	index.Min = int(spec.Min)
	//	index.Max = int(spec.Max)
	//}
	if spec.TextIndexVersion > 0 {
		index.Key = make([]string, len(spec.Weights))
		index.Weights = make(map[string]int)
		for i, elem := range spec.Weights {
			index.Key[i] = "$text:" + elem.Key
			if w, ok := elem.Value.(int32); ok {
				index.Weights[elem.Key] = int(w)
			}
		}
	}

	return index
}

type indexSlice []Index

func (idxs indexSlice) Len() int           { return len(idxs) }
func (idxs indexSlice) Less(i, j int) bool { return idxs[i].Name < idxs[j].Name }
func (idxs indexSlice) Swap(i, j int)      { idxs[i], idxs[j] = idxs[j], idxs[i] }
