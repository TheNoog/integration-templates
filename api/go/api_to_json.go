package main

import (
	"io/ioutil"
	"net/http"
    "os"
)

func check(e error) { if e != nil { panic(e) } }

func main() {

	resp, err := http.Get("https://randomuser.me/api/")  // The API
	check(err)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	check(err)

    f, err := os.Create("data.json")  // The file
	check(err)
    defer f.Close()
    _, err2 := f.WriteString(string(body))
	check(err2)

}