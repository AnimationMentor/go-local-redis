package redis

import "sync"

type Hash map[string]string

var (
    allHashes map[string]Hash = make(map[string]Hash)
    hashesMu  sync.RWMutex
)

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
    defer hashesMu.Unlock()

    existed = 0
    h, exists := allHashes[key]
    if !exists {
        allHashes[key] = Hash{}
        h = allHashes[key]
        existed = 1
    }
    h[field] = value

    publish <- notice{"hash", key, allHashes[key]}

    return
}

// Returns the value associated with field in the hash stored at key.
//
// Return value
// Bulk string reply: the value associated with field, or nil when field is not
// present in the hash or key does not exist.
func HGet(key, field string) string {
    hashesMu.RLock()
    defer hashesMu.RUnlock()

    h := allHashes[key]
    return h[field]
}

// Returns if field is an existing field in the hash stored at key.
//
// Return value
// Integer reply, specifically:
// 1 if the hash contains field.
// 0 if the hash does not contain field, or key does not exist.
func HExists(key, field string) (existed int) {
    hashesMu.RLock()
    defer hashesMu.RUnlock()

    existed = 0

    h, hash_exists := allHashes[key]
    if hash_exists {
        _, field_exists := h[field]
        if field_exists {
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
func Hgetall(key string) (out Hash) {
    hashesMu.RLock()
    defer hashesMu.RUnlock()

    out = Hash{}

    h, _ := allHashes[key]
    for k, v := range h {
        out[k] = v
    }

    return
}

// Returns all values in the hash stored at key.
//
// Return value
// Slice reply: list of values in the hash, or an empty list when key does not exist.
func Hvals(key string) (out []string) {
    hashesMu.RLock()
    defer hashesMu.RUnlock()

    h, _ := allHashes[key]
    for _, v := range h {
        out = append(out, v)
    }

    return
}

// Returns all field names in the hash stored at key.
//
// Return value
// Array reply: list of fields in the hash, or an empty list when key does not exist.
func Hkeys(key string) (out []string) {
    hashesMu.RLock()
    defer hashesMu.RUnlock()

    h, _ := allHashes[key]
    for k, _ := range h {
        out = append(out, k)
    }

    return
}
