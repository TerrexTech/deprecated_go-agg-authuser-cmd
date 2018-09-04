package authuser

import (
	"time"

	"github.com/mongodb/mongo-go-driver/bson/objectid"
)

// Model represents a registered user.
type Model struct {
	ID               objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Name             string            `bson:"name" json:"name"`
	Email            string            `bson:"email,omitempty" json:"email,omitempty"`
	Password         string            `bson:"password,omitempty" json:"password,omitempty"`
	AccountActive    bool              `bson:"account_active,omitempty" json:"account_active,omitempty"`
	RegisterDate     time.Time         `bson:"register_date,omitempty" json:"register_date,omitempty"`
	SuspensionReason string            `bson:"suspension_reason,omitempty" json:"suspension_reason,omitempty"`
}
