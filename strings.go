package redisServer

import "sync"

import (
    "strconv"
)

var (
    allStrings map[string]string = make(map[string]string)
    stringsMu  sync.RWMutex
)

func Set(key, value string) string {
    // Set key to hold the string value. If key already holds a value, it
    // is overwritten, regardless of its type. Any previous time to live
    // associated with the key is discarded on successful SET operation.

    stringsMu.Lock()
    defer stringsMu.Unlock()

    allStrings[key] = value

    publish <- notice{"string", key, allStrings[key]}

    return "OK"
}

func Get(key string) string {
    // Get the value of key. If the key does not exist the special value nil
    // is returned. An error is returned if the value stored at key is not a
    // string, because GET only handles string values.
    //
    // Return value
    // Bulk string reply: the value of key, or nil when key does not exist.

    stringsMu.RLock()
    defer stringsMu.RUnlock()

    return allStrings[key]
}

func Setnx(key, value string) int {
    // Set key to hold string value if key does not exist. In that case,
    // it is equal to SET. When key already holds a value, no operation
    // is performed. SETNX is short for "SET if N ot e X ists".
    //
    // Return value
    // Integer reply, specifically:
    // 1 if the key was set
    // 0 if the key was not set

    stringsMu.Lock()
    defer stringsMu.Unlock()

    _, exists := allStrings[key]
    if exists {
        return 0
    }
    allStrings[key] = value

    publish <- notice{"string", key, allStrings[key]}

    return 1
}

func Incr(key string) string {
    // Increments the number stored at key by one. If the key does not exist,
    // it is set to 0 before performing the operation. An error is returned
    // if the key contains a value of the wrong type or contains a string
    // that can not be represented as integer.
    //
    // Return value
    // String reply: the value of key after the increment

    stringsMu.Lock()
    defer stringsMu.Unlock()

    val, exists := allStrings[key]
    if !exists {
        val = "0"
    }
    i, _ := strconv.Atoi(val)
    allStrings[key] = strconv.Itoa(i + 1)

    publish <- notice{"string", key, allStrings[key]}

    return allStrings[key]
}

func Decr(key string) string {
    // Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer. This operation is limited to 64 bit signed integers.
    // See INCR for extra information on increment/decrement operations.
    //
    // Return value
    // String reply: the value of key after the decrement

    stringsMu.Lock()
    defer stringsMu.Unlock()

    val, exists := allStrings[key]
    if !exists {
        val = "0"
    }
    i, _ := strconv.Atoi(val)
    allStrings[key] = strconv.Itoa(i - 1)

    publish <- notice{"string", key, allStrings[key]}

    return allStrings[key]
}
