package redis

import "regexp"

type notice struct {
	TypeName, KeyName, FieldName string
	Data                         interface{}
}

type consumer struct {
	exps    []*regexp.Regexp
	Channel chan notice
}

var (
	publish      = make(chan notice, 1000)
	consumers    []consumer
	publishCount = 0
)

// Subscribes the client to the given patterns.
// Supported glob-style patterns:
// h?llo subscribes to hello, hallo and hxllo
// h*llo subscribes to hllo and heeeello
// h[ae]llo subscribes to hello and hallo, but not hillo
// Use \ to escape special characters if you want to match them verbatim.
func Psubscribe(pattern ...string) consumer {
	exps := []*regexp.Regexp{}

	for _, p := range pattern {
		r, _ := regexp.Compile(p)
		exps = append(exps, r)
	}
	c = consumer{exps: exps, Channel: make(chan notice, 1000)}
	consumers = append(consumers, c)

	return
}

func init() {
	go func() {
		for v := range publish {
			for _, c := range consumers {
				for _, r := range c.exps {
					if r.MatchString(v.KeyName) == true {
						// fmt.Println("Publishing:", v.KeyName)
						c.Channel <- v
					}
				}
			}
			publishCount++
		}
	}()
}
