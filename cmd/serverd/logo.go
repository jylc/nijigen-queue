package main

import (
	"encoding/base64"
)

const encodedLogo = "ICBfICAgXyBfX18gICAgXyBfX18gX19fXyBfX19fXyBfICAgXyAKIHwgXCB8IHxfIF98ICB8IHxfIF8vIF9fX3wgX19fX3wgXCB8IHwKIHwgIFx8IHx8IHxfICB8IHx8IHwgfCAgX3wgIF98IHwgIFx8IHwKIHwgfFwgIHx8IHwgfF98IHx8IHwgfF98IHwgfF9fX3wgfFwgIHwKIHxffCBcX3xfX19cX19fL3xfX19cX19fX3xfX19fX3xffCBcX3wKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICA="

func logo() string {
	res, _ := base64.StdEncoding.DecodeString(encodedLogo)
	return string(res)
}
