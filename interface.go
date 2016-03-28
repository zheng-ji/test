package bcastkv

type Interface interface {
	Close()
	//Compact() error
	Get(key string, value interface{}) error
	Put(key string, value interface{}) error
	Exist(key string) bool
	Delete(key string) error
}
