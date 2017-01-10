package redis

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
)

var (
	lastPublishCount    uint64 = 0
	DefaultDumpFileName        = "../redisServer.dump.json"
	fileWriteMu         sync.Mutex
)

// Save the DB in background. The OK code is immediately returned. Redis forks, the parent
// continues to serve the clients, the child saves the DB on disk then exits. A client my
// be able to check if the operation succeeded using the LASTSAVE command.
// Please refer to the persistence documentation for detailed information.
//
// Return value
// Simple string reply
func BgSave(fileName string, complete chan bool) string {
	pubCount := atomic.LoadUint64(&publishCount)
	if pubCount > atomic.LoadUint64(&lastPublishCount) {
		atomic.StoreUint64(&lastPublishCount, pubCount)

		go func() {
			fileWriteMu.Lock()
			defer fileWriteMu.Unlock()

			fo, _ := os.Create(fileName)
			defer fo.Close()
			w := bufio.NewWriter(fo)

			// TODO: Lock around each key (checking existence) instead of around the whole
			// hash so that other operations may sneak in.

			hashesMu.Lock()
			b1, err := json.MarshalIndent(&allHashes, "", "    ")
			hashesMu.Unlock()
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b1)

			listsMu.Lock()
			b2, err := json.MarshalIndent(&allLists, "", "    ")
			listsMu.Unlock()
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b2)

			setsMu.Lock()
			b3, err := json.MarshalIndent(&allSets, "", "    ")
			setsMu.Unlock()
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b3)

			stringsMu.Lock()
			b4, err := json.MarshalIndent(&allStrings, "", "    ")
			stringsMu.Unlock()
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b4)

			w.Flush()

			if complete != nil {
				complete <- true
			}
		}()
	}

	return "OK"
}

//// Load any backup before doing anything else.
//
func InitDB(fileName string) {
	if fileName != "" {
		fileWriteMu.Lock()
		defer fileWriteMu.Unlock()

		fo, _ := os.Open(fileName)
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
	}
}
