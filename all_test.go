package redis

//// TODO: Document!

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestHashes(t *testing.T) {
	println(HSet("my first hash", "my key", "yo yo yo"))
	println(HGet("my first hash", "my key"))

	println(HSet("my first hash", "my key", "xerg"))
	println(HGet("my first hash", "my key"))

	println(HExists("my first hash", "not here"))
	println(HExists("my first hash", "my key"))

	println(HSet("my first hash", "his key", "doi"))
	h := Hgetall("my first hash")
	for k, v := range h.ToMap() {
		println(k, ":", v)
	}

	//// Updating the returned hash's value shouldn't effect the source.
	//
	h.Set("his key", "nuht uh")
	for k, v := range Hgetall("my first hash").ToMap() {
		println(k, ":", v)
	}

	for k, v := range Hkeys("my first hash") {
		println(k, ":", v)
	}

	for k, v := range Hvals("my first hash") {
		println(k, ":", v)
	}

}

func TestStrings(t *testing.T) {
	println(Incr("my counter"))
	println(Incr("my counter"))
	println(Incr("my counter"))
	println(Decr("my counter"))
	println(Decr("my counter"))
	println(Decr("my counter"))
	println(Decr("my counter"))

	println(Set("a string", "fun is ok"))
	println(Get("a string"))
	println(Set("a string", "fun is fun"))
	println(Get("a string"))
	println(Setnx("another string", "fresh"))
	println(Get("another string"))
	println(Setnx("another string", "not so fresh"))
	println(Get("another string"))
}

func TestSets(t *testing.T) {
	println(Sadd("a set", "A", "B", "C"))
	println(Sadd("a set", "G", "F", "E", "D", "C", "B", "H"))
	for k, v := range Smembers("a set") {
		println(k, ":", v)
	}
	println(Scard("a set"))
}

func TestLists(t *testing.T) {
	println(Rpush("a list", "X", "Y", "Z"))
	println(Rpush("a list", "G", "F", "E", "D", "C", "B"))
	for k, v := range Lrange("a list", 1, -2) {
		println(k, ":", v)
	}
	for k, v := range Lrange("a list", -5, -3) {
		println(k, ":", v)
	}
	for k, v := range Lrange("a list", -5, -6) {
		println(k, ":", v)
	}
	println(Llen("a list"))
}

func TestKeys(t *testing.T) {
	println(Rpush("a list", "X", "Y", "Z"))
	println(Exists("a list"))
	println(Type("a list"))
	println(Del("a list"))
	println(Exists("a list"))

	println(Incr("my counter"))
	println(HSet("my first hash", "my key", "yo yo yo"))
	println(Rpush("a list", "A", "B", "C"))
	println(Set("a string", "fun is ok"))
	println(Sadd("a set", "X", "Y", "X"))

	for k, v := range Keys(".*") {
		println(k, ":", v)
	}
}

func TestServer(t *testing.T) {
	println(Incr("my counter"))
	println(HSet("my first hash", "my key", "yo yo yo"))
	println(Rpush("a list", "A", "B", "C"))
	println(Set("a string", "fun is ok"))
	println(Sadd("a set", "X", "Y", "X"))

	complete := make(chan bool)
	tmpDir := os.TempDir()
	baseFileName := fmt.Sprintf("localRedisTest.%d.json", os.Getpid())
	fileName := filepath.Join(tmpDir, baseFileName)
	println(BgSave(fileName, complete))
	<-complete
}

func TestPubSubSimple(t *testing.T) {
	var w sync.WaitGroup
	w.Add(2)
	consumer := Psubscribe(".*first.*")

	go func() {
		match := <-consumer.Channel
		hash := match.Data.(Hash)
		println("Match on second consumer:", match.TypeName, match.KeyName, hash.Get("my key"))
		w.Done()
	}()

	go func() {
		println("Setting hash val")
		println(HSet("my first hash", "my key", "yo yo yo"))
		println("Set hash val")
		w.Done()
	}()

	w.Wait()
}

func TestPubSubMultiple(t *testing.T) {
	var w sync.WaitGroup
	w.Add(3)

	go func() {
		consumer := Psubscribe("my first hash", ".*list.*")

		go func() {
			println(Rpush("a list", "item 1", "item 2"))
			println(HSet("my first hash", "my key", "yo yo yo"))
			w.Done()
		}()

		for notice := range consumer.Channel {

			switch data := notice.Data.(type) {
			case List:
				println("Match on consumer:", notice.TypeName, notice.KeyName, data[1])
				w.Done()
			case Hash:
				println("Match on consumer:", notice.TypeName, notice.KeyName, data.Get("my key"))
				w.Done()
			}

		}
	}()

	w.Wait()
}

func TestPubSubValidHash(t *testing.T) {
	key := "TestPubSubValidHash:"
	sub := Psubscribe(fmt.Sprintf("%s.*", key))

	go func() {
		t.Log("HSet")
		HSet(fmt.Sprintf("%s set", key), "field", "value")

		t.Log("HDel")
		HDel(fmt.Sprintf("%s del", key), "field")
	}()

	for i := 0; i < 2; i++ {
		notice := <-sub.Channel
		hash := notice.Data.(Hash)
		if !hash.Valid() {
			t.Errorf("Received an invalid Hash in subscription message: %+v", notice)
			continue
		}
		t.Logf("Received: %+v", notice)
	}
}

func TestPubSubRedisSetUpdates(t *testing.T) {
	var w sync.WaitGroup
	w.Add(1)

	consumer := Psubscribe("publish set")

	go func() {
		println("Setting set vals")
		println(Sadd("publish set", "hot", "doggie"))
		println("Set set vals")
		w.Done()
	}()

	match := <-consumer.Channel
	println("Match on second consumer:", match.TypeName, match.KeyName, match.Data.([]string)[0], match.Data.([]string)[1])

	w.Wait()
}
