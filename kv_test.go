package bcastkv

import (
	"fmt"
	"log"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	kv, err := NewBcastKv("test.kv")
	if err != nil {
		log.Fatal("Can not open database file")
	}
	defer kv.Close()

	key := "keyA"
	err = kv.Put(key, "helloworld")
	if err != nil {
		log.Fatal(err)
	}

	var answer string
	kv.Get(key, &answer)
	fmt.Println(answer)
}
