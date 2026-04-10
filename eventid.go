package relaybox

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	nats "github.com/nats-io/nats.go"
)

// ExtractStableEventID derives a stable identifier from a NATS message.
//
// Resolution order:
// 1. NATS or CloudEvents message headers
// 2. Well-known JSON fields such as "id" or "message_id"
// 3. Deterministic hash of normalized JSON payload bytes
func ExtractStableEventID(msg *nats.Msg) (string, error) {
	if msg == nil {
		return "", errors.New("nil NATS message")
	}

	if msg.Header != nil {
		if headerID := msg.Header.Get(nats.MsgIdHdr); headerID != "" {
			return fmt.Sprintf("nats:%s", headerID), nil
		}
		if headerID := msg.Header.Get("Ce-Id"); headerID != "" {
			return fmt.Sprintf("ce:%s", headerID), nil
		}
		if headerID := msg.Header.Get("ce-id"); headerID != "" {
			return fmt.Sprintf("ce:%s", headerID), nil
		}
	}

	data := msg.Data

	var withID struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &withID); err == nil && withID.ID != "" {
		return withID.ID, nil
	}

	var withMessageID struct {
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal(data, &withMessageID); err == nil && withMessageID.MessageID != "" {
		return fmt.Sprintf("msg:%s", withMessageID.MessageID), nil
	}

	normalized, ok := NormalizeJSONForHash(data)
	if ok {
		data = normalized
	}

	hash := sha256.Sum256(data)
	return fmt.Sprintf("hash:%x", hash[:16]), nil
}

// NormalizeJSONForHash canonicalizes JSON bytes for stable hashing.
//
// If the input is valid JSON, keys are re-encoded in a deterministic order and
// numbers are preserved with json.Number to avoid float conversion drift.
func NormalizeJSONForHash(data []byte) ([]byte, bool) {
	if len(data) == 0 {
		return data, true
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return nil, false
	}

	normalized, err := json.Marshal(payload)
	if err != nil {
		return nil, false
	}

	return normalized, true
}
