package env

import (
	"strings"
)

const (
	Dev     = "DEV"
	Product = "PRODUCT"
)

var (
	env = Dev
)

func Set(v string) {
	upper := strings.ToUpper(v)
	if upper != Dev && upper != Product {
		return
	}

	env = upper
}

func IsDev() bool {
	return env == Dev
}

func IsProduct() bool {
	return env == Product
}
