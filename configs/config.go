package configs

type Options struct {
	MaxMessageNum int32
	MinMessageNum int32
}

func NewOptions() *Options {
	return &Options{
		MaxMessageNum: 20,
		MinMessageNum: 10,
	}
}
