package redis

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

var (
	lastPublishCount = 0
	DefaultDumpFileName       = "../redisServer.dump.json"
	fileWriteMu        sync.Mutex
)

// Save the DB in background. The OK code is immediately returned. Redis forks, the parent
// continues to serve the clients, the child saves the DB on disk then exits. A client my
// be able to check if the operation succeeded using the LASTSAVE command.
// Please refer to the persistence documentation for detailed information.
//
// Return value
// Simple string reply
func BgSave(fileName string, complete chan bool) string {
	if publishCount > lastPublishCount
	if pubCount > lastPublishCount {

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
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b1)
			hashesMu.Unlock()

			listsMu.Lock()
			b2, err := json.MarshalIndent(&allLists, "", "    ")
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b2)
			listsMu.Unlock()

			setsMu.Lock()
			b3, err := json.MarshalIndent(&allSets, "", "    ")
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b3)
			setsMu.Unlock()

			stringsMu.Lock()
			b4, err := json.MarshalIndent(&allStrings, "", "    ")
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b4)
			stringsMu.Unlock()

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
