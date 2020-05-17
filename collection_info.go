package mgo

type CollectionInfo struct {
	// DisableIdIndex prevents the automatic creation of the index
	// on the _id field for the coll.

	// ForceIdIndex enforces the automatic creation of the index
	// on the _id field for the coll. Capped collections,
	// for example, do not have such an index by default.
	ForceIdIndex bool

	// If Capped is true new documents will replace old ones when
	// the coll is full. MaxBytes must necessarily be set
	// to define the size when the coll wraps around.
	// MaxDocs optionally defines the number of documents when it
	// wraps, but MaxBytes still needs to be set.
	Capped   bool
	MaxBytes int
	MaxDocs  int

	// Validator contains a validation expression that defines which
	// documents should be considered valid for this coll.
	Validator interface{}

	// ValidationLevel may be set to "strict" (the default) to force
	// MongoDB to validate all documents on inserts and updates, to
	// "moderate" to apply the validation rules only to documents
	// that already fulfill the validation criteria, or to "off" for
	// disabling validation entirely.
	ValidationLevel string

	// ValidationAction determines how MongoDB handles documents that
	// violate the validation rules. It may be set to "error" (the default)
	// to reject inserts or updates that violate the rules, or to "warn"
	// to log invalid operations but allow them to proceed.
	ValidationAction string

	// StorageEngine allows specifying coll options for the
	// storage engine in use. The map keys must hold the storage engine
	// name for which options are being specified.
	StorageEngine interface{}
	// Specifies the default collation for the coll.
	// Collation allows users to specify language-specific rules for string
	// comparison, such as rules for lettercase and accent marks.
	Collation *Collation
}
