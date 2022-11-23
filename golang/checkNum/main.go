package main
import (
    "fmt"
    "strings"
)

var latitude = []string{
    "-3.347.080",
    "-4.397.029",
    "119.874.190",
    "-0.924",
    "-0.925974",
    "   ",
    "-1.242.721",
}

func PlusZero(num int) string {
	n := 0
	totalZero := ""
	for n < num {
		totalZero = totalZero + "0"
		n++
	}
	return totalZero
}

func main() {
    var newRow []interface{}
    
    for _,value := range latitude {
        fmt.Printf("%s --> ",value)
        if strings.TrimSpace(value) == "" {
            fmt.Printf("\n")
            newRow = append(newRow, nil)
        } else {
            s := strings.Split(value,".")
            fmt.Printf("%s --> ",s)
            if len(s) == 2 {
                fmt.Printf("process on len == 2 > ")
                back := s[1]
                lenBack := len(s[1])
                fmt.Printf("back -> (%s) len %d -->  ",back,lenBack)
                if lenBack < 6 {
                    addition := 6 - lenBack
                    zero := PlusZero(addition)
                    back += zero
                    newValue := s[0] + "." + back
                    fmt.Printf("continue addition (%d), zero (%s) -> newvalue = %s \n",addition,zero,newValue)
                    newRow = append(newRow, newValue)
                } else {
                    fmt.Printf("not continue \n")
                    newRow = append(newRow, value)
                }
            } else {
                fmt.Printf("len more than 2,  value %s \n",value)
                newRow = append(newRow, value)
            }
        }
    }

    fmt.Println("result")
    fmt.Println(newRow...)
}
