package entity

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/petrolax/busdriver"
)

func ValuerHandler(ctx context.Context, event busdriver.Event) error {
	counter := new(Valuer)
	if err := counter.Unmarshal(event.Data); err != nil {
		return fmt.Errorf("failed to unmarshal counter: %w", err)
	}
	log.Printf("value: %d", counter.value)
	return nil
}

type Valuer struct {
	value int `json:"value"`
}

func NewValuer(val int) *Valuer {
	return &Valuer{value: val}
}

func (c *Valuer) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func (c *Valuer) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}
