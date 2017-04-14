package redis

import "sync"

var (
	allHashes = make(map[string]Hash)
	hashesMu  sync.RWMutex
)

// Hash is a concurrent safe string map
type Hash struct {
	m  map[string]string
	mu *sync.RWMutex
}

// NewHash creates a new Hash
func NewHash() Hash {
	return Hash{
		m:  make(map[string]string),
		mu: new(sync.RWMutex),
	}
}

// Valid returns true if the Hash is a valid non-nil instance
func (h Hash) Valid() bool {
	return h.m != nil && h.mu != nil
}

// Size returns the number of items in the hash
func (h Hash) Size() int {
	h.mu.RLock()
	size := len(h.m)
	h.mu.RUnlock()
	return size
}

// Exists returns whether a key exists in the hash
func (h Hash) Exists(key string) bool {
	h.mu.RLock()
	_, ok := h.m[key]
	h.mu.RUnlock()
	return ok
}

// Get returns the value of a key in the map,
// or an empty string if it doesn't exist
func (h Hash) Get(key string) string {
	h.mu.RLock()
	val := h.m[key]
	h.mu.RUnlock()
	return val
}

// GetExists returns the value of a key in the map,
// and whether it existed.
func (h Hash) GetExists(key string) (string, bool) {
	h.mu.RLock()
	val, exists := h.m[key]
	h.mu.RUnlock()
	return val, exists
}

// Set a key to a value
func (h Hash) Set(key, value string) {
	h.mu.Lock()
	h.m[key] = value
	h.mu.Unlock()
}

// Delete a key in the hash
func (h Hash) Delete(key string) {
	h.mu.Lock()
	delete(h.m, key)
	h.mu.Unlock()
}

// Keys returns all keys in the hash
func (h Hash) Keys() []string {
	h.mu.RLock()
	keys := make([]string, 0, len(h.m))
	for k := range h.m {
		keys = append(keys, k)
	}
	h.mu.RUnlock()

	return keys
}

// Values returns all values in the hash
func (h Hash) Values() []string {
	h.mu.RLock()
	values := make([]string, 0, len(h.m))
	for _, v := range h.m {
		values = append(values, v)
	}
	h.mu.RUnlock()

	return values
}

// Copy keys and values to a new Hash
func (h Hash) Copy() Hash {
	newHash := NewHash()

	h.mu.RLock()
	for k, v := range h.m {
		newHash.m[k] = v
	}
	h.mu.RUnlock()

	return newHash
}

// ToMap returns a copy of all data in the hash, as a map
func (h Hash) ToMap() map[string]string {
	h.mu.RLock()
	retMap := make(map[string]string, len(h.m))
	for k, v := range h.m {
		retMap[k] = v
	}
	h.mu.RUnlock()

	return retMap
}

// Sets field in the hash stored at key to value. If key does not exist, a
// new key holding a hash is created. If field already exists in the hash,
// it is overwritten.
//
// Return value
// Integer reply, specifically:
// 1 if field is a new field in the hash and value was set.
// 0 if field already exists in the hash and the value was updated.
func HSet(key, field, value string) (existed int) {
	hashesMu.Lock()

	existed = 0
	h, exists := allHashes[key]
	if !exists {
		h = NewHash()
		allHashes[key] = h
		existed = 1
	}

	hashesMu.Unlock()

	h.Set(field, value)

	publish <- notice{"hash", key, field, h}

	return
}

// Returns the value associated with field in the hash stored at key.
//
// Return value
// Bulk string reply: the value associated with field, or nil when field is not
// present in the hash or key does not exist.
func HGet(key, field string) string {
	hashesMu.RLock()
	h, ok := allHashes[key]
	hashesMu.RUnlock()

	if !ok {
		return ""
	}

	return h.Get(field)
}

// Removes the specified fields from the hash stored at key. Specified fields that do not
// exist within this hash are ignored. If key does not exist, it is treated as an empty
// hash and this command returns 0.
//
// Return value
// Integer reply: the number of fields that were removed from the hash, not including
// specified but non existing fields.
func HDel(key, field string) (existed int) {
	hashesMu.Lock()

	existed = 0
	h, exists := allHashes[key]

	if exists {
		if h.Exists(field) {
			h.Delete(field)
			existed++
		}
	} else {
		// Publish a valid empty Hash
		h = NewHash()
	}

	hashesMu.Unlock()

	publish <- notice{"hash", key, field, h}

	return
}

// Returns if field is an existing field in the hash stored at key.
//
// Return value
// Integer reply, specifically:
// 1 if the hash contains field.
// 0 if the hash does not contain field, or key does not exist.
func HExists(key, field string) (existed int) {
	hashesMu.RLock()
	h, hashExists := allHashes[key]
	hashesMu.RUnlock()

	existed = 0

	if hashExists {
		if h.Exists(field) {
			existed = 1
		}
	}

	return
}

// Returns all fields and values of the hash stored at key. In the returned
// value, every field name is followed by its value, so the length of the reply
// is twice the size of the hash.
//
// Return value
// map[string]string reply: list of fields and their values stored in the hash, or an empty list when key does not exist.
func Hgetall(key string) Hash {
	hashesMu.RLock()
	h, ok := allHashes[key]
	hashesMu.RUnlock()

	if !ok {
		return NewHash()
	}

	return h.Copy()
}

// Returns all values in the hash stored at key.
//
// Return value
// Slice reply: list of values in the hash, or an empty list when key does not exist.
func Hvals(key string) []string {
	hashesMu.RLock()
	h, ok := allHashes[key]
	hashesMu.RUnlock()

	if !ok {
		return []string{}
	}

	return h.Values()
}

// Returns all field names in the hash stored at key.
//
// Return value
// Array reply: list of fields in the hash, or an empty list when key does not exist.
func Hkeys(key string) []string {
	hashesMu.RLock()
	h, ok := allHashes[key]
	hashesMu.RUnlock()

	if !ok {
		return []string{}
	}

	return h.Keys()
}
