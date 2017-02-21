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

			allMaps := make(map[string]map[string]string)

			hashesMu.RLock()
			for key, hash := range allHashes {
				allMaps[key] = hash.ToMap()
			}
			hashesMu.RUnlock()
			b1, err := json.MarshalIndent(&allMaps, "", "    ")
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b1)

			listsMu.RLock()
			b2, err := json.MarshalIndent(&allLists, "", "    ")
			listsMu.RUnlock()
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b2)

			setsMu.RLock()
			b3, err := json.MarshalIndent(&allSets, "", "    ")
			setsMu.RUnlock()
			if err != nil {
				println(err.Error())
				return
			}
			w.Write(b3)

			stringsMu.RLock()
			b4, err := json.MarshalIndent(&allStrings, "", "    ")
			stringsMu.RUnlock()
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

		allMaps := make(map[string]map[string]string)
		dec.Decode(&allMaps)
		hashesMu.Lock()
		for key, aMap := range allMaps {
			hash := NewHash()
			hash.m = aMap
			allHashes[key] = hash
		}
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
