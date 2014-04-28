package redis

import "sync"

type RedisSet map[string]bool

var (
    allSets   map[string]RedisSet = make(map[string]RedisSet)
    setCounts map[string]int      = make(map[string]int)
    setsMu    sync.RWMutex
)

// Add the specified members to the set stored at key. Specified members that are already a member of this set are ignored. If key does not exist, a new set is created before adding the specified members.
// An error is returned when the value stored at key is not a set.
//
// Return value
// Integer reply: the number of elements that were added to the set, not including all the elements already present into the set.
func Sadd(key string, member ...string) (additions int) {
    setsMu.Lock()
    defer setsMu.Unlock()

    s, exists := allSets[key]
    if !exists {
        setCounts[key] = 0
        allSets[key] = RedisSet{}
        s = allSets[key]
    }

    for _, m := range member {
        _, existed := s[m]
        if !existed {
            additions++
            setCounts[key]++
        }
        s[m] = true
    }

    publish <- notice{"set", key, allSets[key]}

    return
}

// Returns all the members of the set value stored at key.
// This has the same effect as running SINTER with one argument key.
//
// Return value
// Array reply: all elements of the set.
func Smembers(key string) (out []string) {
    setsMu.RLock()
    defer setsMu.RUnlock()

    s, _ := allSets[key]
    for k, _ := range s {
        out = append(out, k)
    }
    return
}

// Returns all the members of the set value stored at key.
// This has the same effect as running SINTER with one argument key.
//
// Return value
// Array reply: all elements of the set.
func Scard(key string) (count int) {
    setsMu.RLock()
    defer setsMu.RUnlock()

    count, _ = setCounts[key]

    return
}
