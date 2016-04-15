package redis

import "sync"

type List []string

var (
    allLists map[string]List = make(map[string]List)
    listsMu  sync.RWMutex
)

// Insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the
// push operation. When key holds a value that is not a list, an error is
// returned.
//
// It is possible to push multiple elements using a single command call
// just specifying multiple arguments at the end of the command. Elements
// are inserted one after the other to the tail of the list, from the
// leftmost element to the rightmost element. So for instance the command
// RPUSH mylist a b c will result into a list containing a as first
// element, b as second element and c as third element.

// Return value
// Integer reply: the length of the list after the push operation.
func Rpush(key string, value ...string) int {
    listsMu.Lock()
    defer listsMu.Unlock()

    _, exists := allLists[key]
    if !exists {
        allLists[key] = List{}
    }

    for _, v := range value {
        allLists[key] = append(allLists[key], v)
    }

    publish <- notice{"list", key, "", allLists[key]}

    return len(allLists[key])
}

// Returns the specified elements of the list stored at key. The offsets
// start and stop are zero-based indexes, with 0 being the first element
// of the list (the head of the list), 1 being the next element and so on.
// These offsets can also be negative numbers indicating offsets starting
// at the end of the list. For example, -1 is the last element of the
// list, -2 the penultimate, and so on.

// Consistency with range functions in various programming languages
// Note that if you have a list of numbers from 0 to 100, LRANGE list 0
// 10 will return 11 elements, that is, the rightmost item is included.
// This may or may not be consistent with behavior of range-related
// functions in your programming language of choice (think Ruby's
// Range.new, Array#slice or Python's range() function).

// Out-of-range indexes
// Out of range indexes will not produce an error. If start is larger
// than the end of the list, an empty list is returned. If stop is larger
// than the actual end of the list, Redis will treat it like the last
// element of the list.

// Return value
// Array reply: list of elements in the specified range.
func Lrange(key string, start, stop int) (out List) {
    listsMu.Lock()
    defer listsMu.Unlock()

    out = make(List, 0)

    _, exists := allLists[key]
    if !exists {
        return
    }
    if start < 0 {
        start = len(allLists[key]) + start
    }
    if stop < 0 {
        stop = len(allLists[key]) + stop
    }
    stop++

    out = append(out, allLists[key][start:stop]...)

    return
}

// Returns the length of the list stored at key. If key does not exist,
// it is interpreted as an empty list and 0 is returned. An error is
// returned when the value stored at key is not a list.
//
// Return value
// Integer reply: the length of the list at key.
func Llen(key string) int {
    listsMu.Lock()
    defer listsMu.Unlock()

    _, exists := allLists[key]

    if !exists {
        return 0
    }

    return len(allLists[key])
}
