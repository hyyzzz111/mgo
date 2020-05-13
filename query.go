package mgo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"

	"github.com/yaziming/mgo/bson"
)

type op struct {
	filter   interface{}
	selector interface{}
	sort     interface{}
	limit    int
	skip     int
}
type Query struct {
	collection *mongo.Collection
	op         op
}

func (s *Query) Batch(n int) *Query {
	s.Limit(n)
	return s
}

func (s *Query) One(result interface{}) (err error) {
	single := s.collection.FindOne(context.TODO(), s.op.filter)
	err = single.Err()
	if err != nil {

		return err
	}

	err = single.Decode(result)

	return err
}

func (s *Query) Skip(n int) *Query {
	s.op.skip = n
	return s
}

func (s *Query) Limit(n int) *Query {
	s.op.limit = n
	return s
}
func (s *Query) Select(selector interface{}) *Query {
	s.op.selector = selector
	return s
}
func (s *Query) Sort(fields ...string) *Query {
	var order bson.D
	for _, field := range fields {
		n := 1
		var kind string
		if field != "" {
			if field[0] == '$' {
				if c := strings.Index(field, ":"); c > 1 && c < len(field)-1 {
					kind = field[1:c]
					field = field[c+1:]
				}
			}
			switch field[0] {
			case '+':
				field = field[1:]
			case '-':
				n = -1
				field = field[1:]
			}
		}
		if field == "" {
			panic("Sort: empty field name")
		}
		if kind == "textScore" {
			order = append(order, bson.E{Key: field, Value: bson.M{"$meta": kind}})
		} else {
			order = append(order, bson.E{Key: field, Value: n})
		}
	}
	s.op.sort = order
	return s
}

func (q *Query) All(result interface{}) error {

	opt := options.Find()
	if q.op.sort != nil {
		opt.SetSort(q.op.sort)
	}

	if q.op.limit > 0 {
		opt.SetLimit(int64(q.op.limit))
	}

	opt.SetSkip(int64(q.op.skip))

	cur, err := q.collection.Find(context.TODO(), q.op.filter, opt)
	if err != nil {
		return err
	}
	if err = cur.Err(); err != nil {
		if err == mongo.ErrNilDocument {
			return ErrNotFound
		}
		return err
	}

	err = cur.All(context.TODO(), result)
	return err
}

func (q *Query) Count() (int, error) {
	c, err := q.collection.CountDocuments(context.TODO(), q.op.filter)
	if err != nil {
		return -1, err
	}
	return int(c), nil
}

type Change struct {
	Update    interface{} // The update document
	Upsert    bool        // Whether to insert in case the document isn't found
	Remove    bool        // Whether to remove the document found rather than updating
	ReturnNew bool        // Should the modified document be returned rather than the old one
}

func (q *Query) Apply(change Change, result interface{}) (info *ChangeInfo, err error) {
	if change.Remove {

		r := q.collection.FindOneAndDelete(context.Background(), q.op.filter)
		err = r.Err()
		if err != nil {
			return
		}
		//var doc operation.FindAndModifyResult
		err = r.Decode(&result)
		if err != nil {
			return
		}
		//if result != nil {
		//	err = bson.Unmarshal(doc.Value, result)
		//	if err != nil {
		//		return
		//	}
		//}
		return &ChangeInfo{
			Removed: 1,
			Matched: 1,
		}, nil
	}
	umo := options.FindOneAndUpdate()

	if change.Upsert {
		umo.SetUpsert(true)
	}
	if change.ReturnNew {
		umo.SetReturnDocument(options.After)
	} else {
		umo.SetReturnDocument(options.Before)

	}

	r := q.collection.FindOneAndUpdate(context.Background(), q.op.filter, change.Update, umo)
	if r.Err() != nil {
		err = r.Err()
		return
	}

	err = r.Decode(&result)
	if err != nil {
		return
	}

	return &ChangeInfo{
		Updated: 1,
		//UpsertedId: doc.LastErrorObject.Upserted,
		Matched: 1,
	}, nil
}
