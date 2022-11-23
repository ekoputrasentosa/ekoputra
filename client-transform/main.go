package main

import (
	"fmt"
	"net/rpc"
)

func main() {
    var status string
    client,err := rpc.DialHTTP("tcp","localhost:7070")
    if err != nil {
        fmt.Println(err)
    }

    client.Call("api.Ping","PONG",&status)
}
