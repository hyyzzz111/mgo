package mgo

import (
	"context"
	"github.com/yaziming/mgo/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type Pipe struct {
	pipeline interface{}
	query
	coll *Collection
}

func (p *Pipe) AllowDiskUse() *Pipe {
	p.allowDisk = true
	return p
}
func (p *Pipe) Batch(n int) *Pipe {
	p.op.limit = n
	return p
}
func (p *Pipe) Collation(collation *Collation) *Pipe {
	if collation != nil {
		p.collation = collation
	}
	return p
}
func (p *Pipe) Explain(result interface{}) error {
	command := bson.D{
		{"aggregate", p.coll.collection.Name()},
		{"pipeline", p.pipeline},
		{"explain", true},
	}
	opts := options.RunCmd().SetReadPreference(readpref.Primary())
	if err := p.coll.collection.Database().RunCommand(nil, command, opts).Decode(result); err != nil {
		return err
	}
	return nil
}
func (p *Pipe) aggregate(others ...*options.AggregateOptions) (*mongo.Cursor, error) {
	var ctx context.Context
	if p.maxTimeMS > 0 {
		ctx, _ = context.WithTimeout(context.Background(), time.Duration(p.maxTimeMS)*time.Millisecond)
	}
	opts := p.toAggregateOptions()
	for _, other := range others {
		opts = options.MergeAggregateOptions(opts, other)
	}
	return p.coll.collection.Aggregate(ctx, p.pipeline, opts)
}
func (p *Pipe) All(result interface{}) error {
	cs, err := p.aggregate()

	if err != nil {
		return err
	}
	return cs.All(nil, result)
}
func (p *Pipe) Iter() *Iter {
	cs, err := p.aggregate()
	return &Iter{
		cursor: cs,
		done:   false,
		err:    err,
	}
}
func (p *Pipe) One(result interface{}) (err error) {
	iter := p.Iter()
	if iter.Next(result) {
		return nil
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return ErrNotFound
}
func (p *Pipe) SetMaxTime(d time.Duration) *Pipe {
	p.maxTimeMS = int64(d / time.Millisecond)
	return p
}
