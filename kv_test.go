package bcastkv

import (
	"fmt"
	"log"
	"strconv"
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

func Benchmark_Put(b *testing.B) {
	b.StopTimer()
	kv, err := NewBcastKv("test.kv")
	if err != nil {
		log.Fatal("Can not open database file")
	}
	defer kv.Close()

	b.StartTimer() //重新开始时间
	for i := 0; i < b.N; i++ {
		key := "key_" + strconv.Itoa(i)
		val := "val_" + strconv.Itoa(i)
		err = kv.Put(key, val)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func Benchmark_Get(b *testing.B) {
	b.StopTimer()
	kv, err := NewBcastKv("test.kv")
	if err != nil {
		log.Fatal("Can not open database file")
	}
	defer kv.Close()

	b.StartTimer() //重新开始时间
	var answer string
	for i := 0; i < b.N; i++ {
		key := "key_" + strconv.Itoa(i)
		err = kv.Get(key, &answer)
		if err != nil {
			log.Fatal(err)
		}
	}
}
