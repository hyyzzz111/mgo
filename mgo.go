package mgo

import (
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type Mode = readpref.Mode
type Safe = writeconcern.WriteConcern
type Collation = options.Collation
const (
	Primary = readpref.PrimaryMode
	PrimaryPreferred = readpref.PrimaryPreferredMode
	Secondary = readpref.SecondaryMode
	SecondaryPreferred = readpref.SecondaryPreferredMode
	Nearest = readpref.NearestMode
)
