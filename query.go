package mgo

import (
	"context"
	"errors"
	"github.com/Masterminds/semver"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"reflect"
	"strings"
	"time"

	"github.com/yaziming/mgo/bson"
)

type op struct {
	filter   interface{}
	selector interface{}
	sort     interface{}
	limit    int
	skip     int
	comment  string
	hint     bson.D
}

type query struct {
	collation *Collation
	maxTimeMS int64
	op        op
	allowDisk bool
}

func (q *query) toFindAndDeleteOptions() *options.FindOneAndDeleteOptions {
	opts := options.FindOneAndDelete()
	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.maxTimeMS != 0 {
		opts.SetMaxTime(time.Duration(q.maxTimeMS))
	}

	if len(q.op.hint) > 0 {
		opts.SetHint(q.op.hint)
	}

	if q.op.selector != nil {
		opts.SetProjection(q.op.selector)
	}
	if q.op.sort != nil {
		opts.SetSort(q.op.sort)
	}

	return opts
}
func (q *query) toFindOneAndUpdateOptions() *options.FindOneAndUpdateOptions {
	opts := options.FindOneAndUpdate()
	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.maxTimeMS != 0 {
		opts.SetMaxTime(time.Duration(q.maxTimeMS))
	}

	if len(q.op.hint) > 0 {
		opts.SetHint(q.op.hint)
	}
	if q.op.selector != nil {
		opts.SetProjection(q.op.selector)
	}
	if q.op.sort != nil {
		opts.SetSort(q.op.sort)
	}
	return opts
}
func (q *query) toFindOneAndReplaceOptions() *options.FindOneAndReplaceOptions {
	opts := options.FindOneAndReplace()
	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.maxTimeMS != 0 {
		opts.SetMaxTime(time.Duration(q.maxTimeMS))
	}

	if len(q.op.hint) > 0 {
		opts.SetHint(q.op.hint)
	}

	if q.op.selector != nil {
		opts.SetProjection(q.op.selector)
	}
	if q.op.sort != nil {
		opts.SetSort(q.op.sort)
	}

	return opts
}
func (q *query) toAggregateOptions() *options.AggregateOptions {
	opts := options.Aggregate()
	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.maxTimeMS != 0 {
		opts.SetMaxTime(time.Duration(q.maxTimeMS))
	}
	if q.op.limit > 0 {
		opts.SetBatchSize(int32(q.op.limit))
	}

	if len(q.op.hint) > 0 {
		opts.SetHint(q.op.hint)
	}
	if q.op.comment != "" {
		opts.SetComment(q.op.comment)
	}

	if q.allowDisk {
		opts.SetAllowDiskUse(q.allowDisk)
	}
	return opts
}
func (q *query) toFindOptions() *options.FindOptions {
	opts := options.Find()
	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.maxTimeMS != 0 {
		opts.SetMaxTime(time.Duration(q.maxTimeMS))
	}

	if q.op.skip > 0 {
		opts.SetSkip(int64(q.op.skip))
	}
	if len(q.op.hint) > 0 {
		opts.SetHint(q.op.hint)
	}
	if q.op.comment != "" {
		opts.SetComment(q.op.comment)
	}
	if q.op.selector != nil {
		opts.SetProjection(q.op.selector)
	}
	if q.op.sort != nil {
		opts.SetSort(q.op.sort)
	}
	if q.op.limit > 0 {
		opts.SetLimit(int64(q.op.limit))
	}
	if q.allowDisk {
		opts.SetAllowDiskUse(q.allowDisk)
	}
	return opts
}
func (q *query) toDistinctOptions() *options.DistinctOptions {
	opts := options.Distinct()
	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.maxTimeMS != 0 {
		opts.SetMaxTime(time.Duration(q.maxTimeMS))
	}
	return opts
}
func (q *query) toCountOptions() *options.CountOptions {
	opts := options.Count()
	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.maxTimeMS != 0 {
		opts.SetMaxTime(time.Duration(q.maxTimeMS))
	}

	if q.op.skip > 0 {
		opts.SetSkip(int64(q.op.skip))
	}
	if len(q.op.hint) > 0 {
		opts.SetHint(q.op.hint)
	}
	if q.op.limit > 0 {
		opts.SetLimit(int64(q.op.limit))
	}
	return opts
}

func (q *query) toFindOneOptions() *options.FindOneOptions {
	opts := options.FindOne()
	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.maxTimeMS != 0 {
		opts.SetMaxTime(time.Duration(q.maxTimeMS))
	}
	if q.op.skip > 0 {
		opts.SetSkip(int64(q.op.skip))
	}
	if len(q.op.hint) > 0 {
		opts.SetHint(q.op.hint)
	}
	if q.op.comment != "" {
		opts.SetComment(q.op.comment)
	}
	if q.op.selector != nil {
		opts.SetProjection(q.op.selector)
	}
	if q.op.sort != nil {
		opts.SetSort(q.op.sort)
	}

	return opts
}

type Query struct {
	query
	coll *Collection
	err  error
}

var (
	allowDiskUseConstraint, _ = semver.NewConstraint("<=4.4,>=3.2")
)

func (qr *Query) AllowDiskUse() *Query {
	if allowDiskUseConstraint.Check(qr.coll.db.version) {
		qr.query.allowDisk = true
	}
	return qr
}
func (qr *Query) Batch(n int) *Query {

	qr.Limit(n)
	return qr
}
func (qr *Query) Collation(collation *Collation) *Query {
	qr.collation = collation
	return qr
}
func (qr *Query) One(result interface{}) (err error) {
	if qr.err != nil {
		err = qr.err
		return
	}
	opts := qr.toFindOneOptions()
	sg := qr.coll.collection.FindOne(nil, qr.op.filter, opts)
	if sg.Err() != nil {
		return sg.Err()
	}
	if result == nil {
		return
	}
	return sg.Decode(result)
}

func (qr *Query) Skip(n int) *Query {
	qr.op.skip = n
	return qr
}

func (qr *Query) Limit(n int) *Query {
	qr.op.limit = n
	return qr
}
func (qr *Query) Select(selector interface{}) *Query {
	qr.op.selector = selector
	return qr
}
func (qr *Query) Sort(fields ...string) *Query {
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
	qr.op.sort = order
	return qr
}
func (qr *Query) cursor(others ...*options.FindOptions) (cur *mongo.Cursor, err error) {
	if qr.err != nil {
		err = qr.err
		return
	}

	opts := qr.toFindOptions()
	for _, other := range others {
		opts = options.MergeFindOptions(opts, other)
	}
	cur, err = qr.coll.collection.Find(nil, qr.op.filter, opts)
	if err != nil {
		return
	}
	return cur, cur.Err()
}
func (qr *Query) Comment(comment string) *Query {
	qr.op.comment = comment
	return qr
}
func (qr *Query) SetMaxTime(d time.Duration) *Query {
	qr.maxTimeMS = int64(d / time.Millisecond)
	return qr
}
func (qr *Query) Hint(indexKey ...string) *Query {
	keyInfo, err := parseIndexKey(indexKey)
	qr.err = err

	if err != nil {
		return qr
	}
	qr.op.hint = keyInfo.key
	return qr
}
func (qr *Query) Explain(result interface{}) error {
	if qr.op.selector == nil {
		qr.op.selector = bson.D{}
	}
	findCmd := bson.D{
		{"find", qr.coll.collection.Name()},
		{"filter", qr.op.filter},
	}
	if qr.op.limit > 0 {
		findCmd = append(findCmd, bson.E{Key: "limit", Value: qr.op.limit})
	}
	command := bson.D{{"explain", findCmd}}

	opts := options.RunCmd().SetReadPreference(readpref.Primary())
	if err := qr.coll.db.database.RunCommand(nil, command, opts).Decode(result); err != nil {
		return err
	}
	return nil
}
func (qr *Query) Distinct(key string, result interface{}) (err error) {

	docs, err := qr.coll.collection.Distinct(nil, key, qr.op.filter, qr.toDistinctOptions())
	resultsVal := reflect.ValueOf(result)
	if resultsVal.Kind() != reflect.Ptr {
		return errors.New("results argument must be a pointer to a slice")
	}
	sliceVal := resultsVal.Elem()
	elementType := sliceVal.Type().Elem()
	var index = 0
	for _, doc := range docs {
		if sliceVal.Len() == index {
			// slice is full
			newElem := reflect.New(elementType)
			sliceVal = reflect.Append(sliceVal, newElem.Elem())
			sliceVal = sliceVal.Slice(0, sliceVal.Cap())
		}

		currElem := sliceVal.Index(index).Addr()
		currElem.Elem().Set(reflect.ValueOf(doc).Convert(elementType))
		index++
	}
	resultsVal.Elem().Set(sliceVal.Slice(0, index))
	return err
}
func (qr *Query) All(result interface{}) (err error) {
	cur, err := qr.cursor()
	if err != nil {
		return err
	}
	if cur.Err() != nil {
		return cur.Err()
	}
	if result == nil {
		return
	}
	return cur.All(nil, result)
}

func (qr *Query) Count() (int, error) {
	if qr.err != nil {

		return -1, qr.err
	}
	opts := qr.toCountOptions()
	c, err := qr.coll.collection.CountDocuments(nil, qr.op.filter, opts)
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

func (qr *Query) Apply(change Change, result interface{}) (info *ChangeInfo, err error) {
	if qr.err != nil {
		err = qr.err
		return
	}
	if change.Remove {
		opts := qr.toFindAndDeleteOptions()
		r := qr.coll.collection.FindOneAndDelete(context.Background(), qr.op.filter, opts)
		err = r.Err()
		if err != nil {
			return
		}
		err = r.Decode(&result)
		if err != nil {
			return
		}

		return &ChangeInfo{
			Removed: 1,
			Matched: 1,
		}, nil
	}
	uro := qr.toFindOneAndReplaceOptions()
	if change.Upsert {
		uro.SetUpsert(true)
	}
	if change.ReturnNew {
		uro.SetReturnDocument(options.After)
	} else {
		uro.SetReturnDocument(options.Before)
	}
	replaceResult := qr.coll.collection.FindOneAndReplace(nil, qr.op.filter, change.Update, uro)
	if replaceResult.Err() == nil {
		if err = replaceResult.Decode(result); err != nil {
			return nil, err
		}
		return &ChangeInfo{
			Updated: 1,
			//UpsertedId: doc.LastErrorObject.Upserted,
			Matched: 0,
		}, nil
	}
	if replaceResult.Err().Error() != "replacement document cannot contain keys beginning with '$'" {
		return nil, replaceResult.Err()
	}

	umo := qr.toFindOneAndUpdateOptions()
	if change.Upsert {
		umo.SetUpsert(true)
	}
	if change.ReturnNew {
		umo.SetReturnDocument(options.After)
	} else {
		umo.SetReturnDocument(options.Before)
	}
	r := qr.coll.collection.FindOneAndUpdate(context.Background(), qr.op.filter, change.Update, umo)
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
		Matched: 0,
	}, nil
}

func (qr *Query) Iter() *Iter {
	cur, err := qr.cursor()
	return &Iter{cursor: cur, err: err}
}

type Iter struct {
	cursor *mongo.Cursor
	done   bool
	err    error
}

func (iter *Iter) All(result interface{}) error {
	if iter.err != nil {
		return iter.err
	}
	if iter.err = iter.cursor.Err(); iter.err != nil {
		return iter.err
	}
	iter.err = iter.cursor.All(nil, result)
	return iter.err
}
func (iter *Iter) Close() error {
	if iter.err != nil {
		return iter.err
	}
	return iter.cursor.Close(nil)
}
func (iter *Iter) Err() error {
	return iter.err
}

func (iter *Iter) Done() bool {
	return iter.err != nil || iter.done
}
func (iter *Iter) Next(result interface{}) bool {
	if iter.err != nil {
		return false
	}
	iter.done = !iter.cursor.Next(nil)
	if !iter.done {
		iter.err = iter.cursor.Decode(result)
	}
	return !iter.done
}

// QueryError is returned when a query fails
type QueryError struct {
	Code      int
	Message   string
	Assertion bool
}

func (err *QueryError) Error() string {
	return err.Message
}
