package mgo

import (
	"context"
	"reflect"
	
	"go.mongodb.org/mongo-driver/mongo"
)

type Pipe struct {
	pipeline   interface{}
	collection *mongo.Collection
}

func (p *Pipe) All(result interface{}) error {
	cs, err := p.collection.Aggregate(context.TODO(), p.pipeline)
	//cs, err := p.collection.Watch(context.TODO(),p.pipeline)
	
	if err != nil {
		return err
	}
	
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr {
		panic("result argument must be a slice address")
	}
	
	slicev := resultv.Elem()
	
	if slicev.Kind() == reflect.Interface {
		slicev = slicev.Elem()
	}
	if slicev.Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}
	
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()
	
	i := 0
	
	for {
		if slicev.Len() == i {
			elemp := reflect.New(elemt)
			if !cs.Next(context.TODO()) {
				break
			}
			err = cs.Decode(elemp.Interface())
			if err != nil {
				return err
			}
			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			if !cs.Next(context.TODO()) {
				break
			}
			if cs.Decode(slicev.Index(i).Addr().Interface()) != nil {
				break
			}
		}
		i++
	}
	resultv.Elem().Set(slicev.Slice(0, i))
	return cs.Close(context.TODO())
	
}

func (p *Pipe) One(reslut interface{}) (err error) {
	
	return nil
}
