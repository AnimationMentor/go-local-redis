package redis

import "regexp"

// Removes the specified keys. A key is ignored if it does not exist.
//
// Return value
// Integer reply: The number of keys that were removed.
func Del(key ...string) (deletedCount int) {

    hashesMu.Lock()
    defer hashesMu.Unlock()

    listsMu.Lock()
    defer listsMu.Unlock()

    setsMu.Lock()
    defer setsMu.Unlock()

    stringsMu.Lock()
    defer stringsMu.Unlock()

    deletedCount = 0

    for _, k := range key {
        if _, exists := allHashes[k]; exists {
            delete(allHashes, k)
            deletedCount++
            publish <- notice{"hash", k, nil}
            continue
        }
        if _, exists := allLists[k]; exists {
            delete(allLists, k)
            deletedCount++
            publish <- notice{"list", k, nil}
            continue
        }
        if _, exists := allSets[k]; exists {
            delete(allSets, k)
            deletedCount++
            publish <- notice{"set", k, nil}
            continue
        }
        if _, exists := allStrings[k]; exists {
            delete(allStrings, k)
            deletedCount++
            publish <- notice{"string", k, nil}
            continue
        }
    }

    return
}

// Returns if key exists.
//
// Return value
// Integer reply, specifically:
// 1 if the key exists.
// 0 if the key does not exist.
func Exists(key string) int {

    hashesMu.Lock()
    defer hashesMu.Unlock()

    listsMu.Lock()
    defer listsMu.Unlock()

    setsMu.Lock()
    defer setsMu.Unlock()

    stringsMu.Lock()
    defer stringsMu.Unlock()

    if _, exists := allHashes[key]; exists {
        return 1
    }
    if _, exists := allLists[key]; exists {
        return 1
    }
    if _, exists := allSets[key]; exists {
        return 1
    }
    if _, exists := allStrings[key]; exists {
        return 1
    }

    return 0
}

// Returns the string representation of the type of the value stored at key. The different
// types that can be returned are: string, list, set, zset and hash.
//
// Return value
// Simple string reply: type of key, or none when key does not exist.
func Type(key string) string {

    hashesMu.Lock()
    defer hashesMu.Unlock()

    listsMu.Lock()
    defer listsMu.Unlock()

    setsMu.Lock()
    defer setsMu.Unlock()

    stringsMu.Lock()
    defer stringsMu.Unlock()

    if _, exists := allHashes[key]; exists {
        return "hash"
    }
    if _, exists := allLists[key]; exists {
        return "list"
    }
    if _, exists := allSets[key]; exists {
        return "set"
    }
    if _, exists := allStrings[key]; exists {
        return "string"
    }

    return ""
}

// Returns all keys matching pattern.
//
// While the time complexity for this operation is O(N), the constant times are fairly
// low. For example, Redis running on an entry level laptop can scan a 1 million key
// database in 40 milliseconds.
// Warning: consider KEYS as a command that should only be used in production
// environments with extreme care. It may ruin performance when it is executed against
// large databases. This command is intended for debugging and special operations, such
// as changing your keyspace layout. Don't use KEYS in your regular application code.
// If you're looking for a way to find keys in a subset of your keyspace, consider using
// sets.
// Supported glob-style patterns:
// h?llo matches hello, hallo and hxllo
// h*llo matches hllo and heeeello
// h[ae]llo matches hello and hallo, but not hillo
// Use \ to escape special characters if you want to match them verbatim.
//
// Return value
// Array reply: list of keys matching pattern.
func Keys(pattern string) (out []string) {

    hashesMu.Lock()
    defer hashesMu.Unlock()

    listsMu.Lock()
    defer listsMu.Unlock()

    setsMu.Lock()
    defer setsMu.Unlock()

    stringsMu.Lock()
    defer stringsMu.Unlock()

    r, _ := regexp.Compile(pattern)

    for k, _ := range allHashes {
        if r.MatchString(k) == true {
            out = append(out, k)
        }
    }

    for k, _ := range allLists {
        if r.MatchString(k) == true {
            out = append(out, k)
        }
    }

    for k, _ := range allSets {
        if r.MatchString(k) == true {
            out = append(out, k)
        }
    }

    for k, _ := range allStrings {
        if r.MatchString(k) == true {
            out = append(out, k)
        }
    }

    return
}
