package main

import (
    "fmt"
    "strings"
    "os"
    "bufio"
)

func cleanText(text string)  string {
    text = strings.TrimSpace(text)
    text = strings.Trim(text, `"`)
    text = strings.TrimSpace(text)
    return text
}

func cleanRow(strList []string) []string{
    var newRow []string
    for _, entry := range strList {
        entry = cleanText(entry)
        newRow = append(newRow, entry)
    }
    return newRow
}

func main() {
    file,err := os.Open("list_extract.txt")
    if err!=nil{
        fmt.Println(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        text := scanner.Text()
    }
}
