package mgo

import (
	"bytes"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"mgo/bson"
)

type Bulk struct {
	c       *Collection
	models  []mongo.WriteModel
	ordered bool
}

func (b *Bulk) Unordered() {
	b.ordered = false
}
func (b *Bulk) Insert(docs ...interface{}) {
	for _, doc := range docs {
		model := mongo.NewInsertOneModel().SetDocument(doc)
		b.models = append(b.models, model)
	}
}
func (b *Bulk) Remove(selectors ...interface{}) {
	for _, selector := range selectors {
		if selector == nil {
			selector = bson.D{}
		}
		model := mongo.NewDeleteOneModel().SetFilter(selector)
		b.models = append(b.models, model)
	}
}
func (b *Bulk) RemoveAll(selectors ...interface{}) {
	for _, selector := range selectors {
		if selector == nil {
			selector = bson.D{}
		}
		model := mongo.NewDeleteManyModel().SetFilter(selector)
		b.models = append(b.models, model)
	}
}
func (b *Bulk) Update(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.Update requires an even number of parameters")
	}
	for i := 0; i < len(pairs); i += 2 {
		selector := pairs[i]
		if selector == nil {
			selector = bson.D{}
		}
		model := mongo.NewUpdateOneModel().SetFilter(selector).
			SetUpdate(pairs[i+1])
		b.models = append(b.models, model)
	}
}
func (b *Bulk) UpdateAll(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.UpdateAll requires an even number of parameters")
	}
	for i := 0; i < len(pairs); i += 2 {
		selector := pairs[i]
		if selector == nil {
			selector = bson.D{}
		}
		model := mongo.NewUpdateManyModel().SetFilter(selector).
			SetUpdate(pairs[i+1])
		b.models = append(b.models, model)
	}
}
func (b *Bulk) Upsert(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.Update requires an even number of parameters")
	}
	for i := 0; i < len(pairs); i += 2 {
		selector := pairs[i]
		if selector == nil {
			selector = bson.D{}
		}
		model := mongo.NewUpdateOneModel().SetFilter(selector).
			SetUpdate(pairs[i+1]).
			SetUpsert(true)
		b.models = append(b.models, model)
	}
}
func (b *Bulk) Run(others ...*options.BulkWriteOptions) (br *BulkResult, bulkerr error) {
	opts := options.BulkWrite().SetOrdered(b.ordered)

	for _, other := range others {
		opts = options.MergeBulkWriteOptions(opts, other)
	}
	result, err := b.c.collection.BulkWrite(nil, b.models, opts)
	if err != nil {
		var bulkError = &BulkError{ecases: []BulkErrorCase{}}
		switch newType := err.(type) {
		case mongo.BulkWriteException:
			for _, e := range newType.WriteErrors {
				bulkError.ecases = append(bulkError.ecases, BulkErrorCase{
					Index: e.Index,
					Err:   e,
				})
			}
		}
		return br, bulkError
	}
	return &BulkResult{
		Matched:   int(result.MatchedCount),
		Modified:  int(result.ModifiedCount),
		Inserted:  int(result.InsertedCount),
		Deleted:   int(result.DeletedCount),
		Upserted:  int(result.UpsertedCount),
		UpsertIds: result.UpsertedIDs,
	}, nil
}

// BulkResult holds the results for a bulk operation.
type BulkResult struct {
	Matched   int
	Modified  int // Available only for MongoDB 2.6+
	Inserted  int
	UpsertIds map[int64]interface{}
	Deleted   int
	Upserted  int
}
type BulkErrorCase struct {
	Index int // Position of operation that failed, or -1 if unknown.
	Err   error
}

func (e *BulkError) Cases() []BulkErrorCase {
	return e.ecases
}
func (e *BulkError) Error() string {
	if len(e.ecases) == 0 {
		return "invalid BulkError instance: no errors"
	}
	if len(e.ecases) == 1 {
		return e.ecases[0].Err.Error()
	}
	msgs := make([]string, 0, len(e.ecases))
	seen := make(map[string]bool)
	for _, ecase := range e.ecases {
		msg := ecase.Err.Error()
		if !seen[msg] {
			seen[msg] = true
			msgs = append(msgs, msg)
		}
	}
	if len(msgs) == 1 {
		return msgs[0]
	}
	var buf bytes.Buffer
	buf.WriteString("multiple errors in bulk operation:\n")
	for _, msg := range msgs {
		buf.WriteString("  - ")
		buf.WriteString(msg)
		buf.WriteByte('\n')
	}
	return buf.String()
}

// BulkError holds an error returned from running a Bulk operation.
// Individual errors may be obtained and inspected via the Cases method.
type BulkError struct {
	ecases []BulkErrorCase
}
