package redis

import (
    "bufio"
    "encoding/json"
    "os"
    "sync"
)

var (
    lastPublishCount = 0
    DumpFileName     = "../redisServer.dump.json"
    fileWriteMu      sync.Mutex
)

// Save the DB in background. The OK code is immediately returned. Redis forks, the parent
// continues to serve the clients, the child saves the DB on disk then exits. A client my
// be able to check if the operation succeeded using the LASTSAVE command.
// Please refer to the persistence documentation for detailed information.
//
// Return value
// Simple string reply
func BgSave(complete chan bool) string {
    if publishCount > lastPublishCount {
        lastPublishCount = publishCount

        go func() {
            fileWriteMu.Lock()
            defer fileWriteMu.Unlock()

            fo, _ := os.Create(DumpFileName)
            defer fo.Close()
            w := bufio.NewWriter(fo)
            enc := json.NewEncoder(w)

            // TODO: Lock around each key (checking existence) instead of around the whole
            // hash so that other operations may sneak in.

            hashesMu.Lock()
            enc.Encode(&allHashes)
            hashesMu.Unlock()

            listsMu.Lock()
            enc.Encode(&allLists)
            listsMu.Unlock()

            setsMu.Lock()
            enc.Encode(&allSets)
            setsMu.Unlock()

            stringsMu.Lock()
            enc.Encode(&allStrings)
            stringsMu.Unlock()

            w.Flush()

            if complete != nil {
                complete <- true
            }
        }()
    }

    return "OK"
}

func init() {
    go func() {
        fileWriteMu.Lock()
        defer fileWriteMu.Unlock()

        fo, _ := os.Open(DumpFileName)
        defer fo.Close()

        r := bufio.NewReader(fo)
        dec := json.NewDecoder(r)

        hashesMu.Lock()
        dec.Decode(&allHashes)
        hashesMu.Unlock()

        listsMu.Lock()
        dec.Decode(&allLists)
        listsMu.Unlock()

        setsMu.Lock()
        dec.Decode(&allSets)
        setsMu.Unlock()

        stringsMu.Lock()
        dec.Decode(&allStrings)
        stringsMu.Unlock()
    }()
}
