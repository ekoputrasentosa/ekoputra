package main

import (
    "fmt"
    "net/http"
    "sync"
)

func main() {
    wg := &sync.WaitGroup{}
    
    fmt.Println("Get status code of list of web")
    listWebsites := []string{
        "https://fb.com",
        "https://google.com",
        "https://gitlab.com",
        "https://github.com",
        "https://eko.ps",
        "https://stackoverflow.com",
        "https://youtube.com",
    }

    for _,web := range listWebsites {
        wg.Add(1)
        go getStatusWeb(web,wg)
    }

    wg.Wait()
    fmt.Println("Status Finished")
}

func getStatusWeb(web string, wg *sync.WaitGroup){
    defer wg.Done()
    res, err := http.Get(web)
    if err != nil {
        fmt.Printf("Cannot access for -> %v, status code: (%v)\n", web,err)
    } else {
        fmt.Printf("Success for -> %v, status code: %v\n",web,res.StatusCode)
    }
}
