package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ran4GHuaIDList = [12]string{ // the array size MUST EXACTLY match with its content size
	"start_timestamp",
	"finish_timestamp",
	"data_type",
	"reliability",
	"ne_name",
	"enodeb_function_name",
	"enodeb_id",
	"cell_name",
	"cell_id",
	"fdd_tdd",
	"ip_source",
	"regional",
}

var ran4GHuaTimeColumn1 = "start_timestamp"
var ran4GHuaTimeColumn2 = "finish_timestamp"

type ran4GHuaDataRow struct {
	key   [len(ran4GHuaIDList)]interface{}
	value map[string]float64
}

var siteNameReplaceList = [...]string{
	"R_R_R_",
	"C_N_",
	"C_S_",
	"R_C_",
	"R_S_",
	"N_C_",
	"C_C_",
	"D_N_",
	"N_R_",
	"C_R_",
	"R_R_",
	"R_N_",
	"N_N_",
	"16_",
	"JN_",
	"NP_",
	"NR_",
	"NS_",
	"N _",
	"N_ ",
	"C_ ",
	"NC_",
	"E_",
	"E-",
	"D#",
	"E#",
	"C-",
	"N-",
	"R-",
	"S-",
	"C_",
	"S_",
	"R_",
	"N_",
	"V_",
	"M_",
	"H_",
	"T_",
	"D_",
	"_",
	" ",
}

type ran4GHuaCSVReader struct {
	parserPut  func([]string, map[string]int)
	readerWg   sync.WaitGroup
	readerChan chan string
	fileList   []string
	err        error
}

func NeutralizeName(name string) string {
	replacedName := strings.Replace(name, ".", "_", -1)
	replacedName = strings.Replace(replacedName, "(LTE)", "LTE", -1)
	replacedName = strings.Replace(replacedName, "-", "_", -1)
	replacedName = strings.Replace(replacedName, ",", "_", -1)
	replacedName = strings.Replace(replacedName, "~", "_", -1)
	replacedName = strings.Replace(replacedName, "%", "pct", -1)
	replacedName = strings.Replace(replacedName, "#", "amt", -1)
	replacedName = strings.Replace(replacedName, "(", "_", -1)
	replacedName = strings.Replace(replacedName, ")", "_", -1)
	replacedName = strings.Replace(replacedName, "[", "_", -1)
	replacedName = strings.Replace(replacedName, "]", "_", -1)
	replacedName = strings.Replace(replacedName, " ", "_", -1)
	replacedName = strings.Replace(replacedName, ":", "_", -1)
	replacedName = strings.Replace(replacedName, "/", "_", -1)
	replacedName = strings.Replace(replacedName, "&", "_", -1)
	replacedName = strings.Replace(replacedName, "+", "_", -1)
	replacedName = strings.Replace(replacedName, "\n", "_", -1)
	if strings.HasPrefix(replacedName, "4") {
		replacedName = strings.Replace(replacedName, "4", "Four", 1)
	}
	return replacedName
}

func NeutralizeSiteName(name string) string {
	for _, prefix := range siteNameReplaceList {
		if strings.HasPrefix(name, prefix) {
			name = strings.Replace(name, prefix, "", 1)
			break
		}
	}
	return name
}

func ParseObjectName(objectName string) map[string]string {
	result := make(map[string]string)
	strProcess1 := strings.Split(objectName, "/")
	result["NEName"] = NeutralizeSiteName(strProcess1[0])
	for _, data := range strProcess1[1:] {
		strProcess2 := strings.Split(data, ":")
		if len(strProcess2) == 2 {
			if strProcess2[0] == "LocalIP" || strProcess2[0] == "PeerIP" || strProcess2[0] == "PHB" {
				result[strProcess2[0]] = strProcess2[1]
			} else {
				result["DataType"] = strProcess2[0]
				properties := strings.Split(strProcess2[1], ",")
				for _, property := range properties {
					property = strings.TrimSpace(property)
					propertySplit := strings.Split(property, "=")
					if len(propertySplit) == 2 {
						if propertySplit[0] == "NodeB Function Name" ||
							propertySplit[0] == "GBTS Function Name" ||
							propertySplit[0] == "BTS Name" ||
							propertySplit[0] == "eNodeB Function Name" ||
							propertySplit[0] == "Cell Name" {
							result[propertySplit[0]] = NeutralizeSiteName(propertySplit[1])
						} else if propertySplit[0] == "Adjacent node ID" {
							result["Adjacent Node ID"] = propertySplit[1]
						} else if propertySplit[0] == "Port Cabinet No." {
							result["Cabinet No."] = propertySplit[1]
						} else if propertySplit[0] == "Port Subrack No." {
							result["Subrack No."] = propertySplit[1]
						} else if propertySplit[0] == "Port Slot No." {
							result["Slot No."] = propertySplit[1]
						} else {
							result[propertySplit[0]] = propertySplit[1]
						}
					}
				}
			}
		}
	}
	return result
}

func main() {
    file,err := os.Open("files/example.csv")    
    if err != nil {
        fmt.Println(err)
    }
    defer file.Close()

    reader := csv.NewReader(file)
    var lineNumber int
    var droppedLine int
    var lineLength int

    headerMap := make(map[string]int)

    for {
        row, err := reader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            fmt.Println(err)
        }
        if lineNumber == 0 {
            for j, header := range row {
                header = NeutralizeName(header)
                // fmt.Println("header --",header)
                headerMap[header] = j
            }
            lineLength = len(row)
        } else if lineNumber == 1 {
            // skip gajelas
        } else {
            if len(row) != lineLength {
                fmt.Println("This row has drop line")
                lineNumber++
                droppedLine++
                continue
            }
            // fmt.Println("Row content -----\n",row)
             // this is to parser data
            parserProcess(row,headerMap)
        }
        lineNumber++
    }
    fmt.Println("=============================================")
    fmt.Println("\nEND PROCESS")

}

func getData(headerName string, row []string, headerMap map[string]int) string {
   return row[headerMap[headerName]] 
}

func parserProcess(line []string, headerMap map[string]int) {
    idHeaderIndexMap := make(map[string]int)
    for i,col := range ran4GHuaIDList {
       idHeaderIndexMap[col] = i
    }

    // assume counterList taken from DB
    counterHeaderList := readCounterList()
    
    counterHeaderIndexMap := make(map[string]int)
    for index,col := range counterHeaderList {
        counterHeaderIndexMap[col]  = index
    }
    
    const timeLayout string = "2006-01-02 15:04"
    var key [len(ran4GHuaIDList)]interface{}
    valueMap := make(map[string]float64)
    prbMap := make(map[string]float64)

    parsedTime, err := time.Parse(timeLayout, getData("Result_Time",line,headerMap))
    if err != nil {
        fmt.Println("error in parsed time")
        fmt.Println(err) 
    }
    key[idHeaderIndexMap["start_timestamp"]] = parsedTime.Unix() - (7 * 60 * 60)
    granularity, _ := strconv.Atoi(getData("Granularity_Period",line,headerMap))
    objectInfo := ParseObjectName(getData("Object_Name",line,headerMap))
    key[idHeaderIndexMap["ne_name"]] = objectInfo["NEName"]
    key[idHeaderIndexMap["ip_source"]] = "10.138.68.7"
    key[idHeaderIndexMap["finish_timestamp"]] = parsedTime.Unix() - (7 * 60 * 60) + (int64(granularity) * 60)
    key[idHeaderIndexMap["reliability"]] = getData("Reliability",line,headerMap)
    key[idHeaderIndexMap["data_type"]] = objectInfo["DataType"]
    enodebName := objectInfo["eNodeB Function Name"]
    // cellId := objectInfo["Local Cell ID"]
    // enodebId := objectInfo["eNodeB ID"]
    cell_name := objectInfo["Cell Name"]
    // fmt.Println(enodebName, cell_name)
    key[idHeaderIndexMap["enodeb_function_name"]] = enodebName
    key[idHeaderIndexMap["cell_name"]] = cell_name // moved for the sake of GetCellMap using cell_name        
    // pengecekan cellMap
    regional := "JATIM"
    key[idHeaderIndexMap["regional"]] = regional
    prbMap["prb_dl"] = 96
    prbMap["prb_ul"] = 92
    // pengecekan bandwith
    bandwidth := strings.Split("10.0", ".")
    if len(bandwidth) > 0 {
        prbMap["available_prb"] = 21
    }
    key[idHeaderIndexMap["enodeb_id"]] = objectInfo["eNodeB ID"]
    key[idHeaderIndexMap["cell_id"]] = objectInfo["Local Cell ID"]
    key[idHeaderIndexMap["fdd_tdd"]] = objectInfo["Cell FDD TDD indication"]

    for _,counterName := range counterHeaderList {
        if counterName == "prb_ul" || counterName == "prb_dl" || counterName == "available_prb" {
            if prbMap[counterName] > 0 {
                valueMap[counterName] = prbMap[counterName]
            }
        }
        var index int
        if id,ok := headerMap[counterName] ; ok {
            index = id
        } else {
           index = id 
        }

        valStr := strings.Replace(line[index], "%", "", -1)
        valStr = strings.Replace(valStr, ",", "", -1)
        val, err := strconv.ParseFloat(valStr,64)
        if err != nil {
            continue
        }

        valueMap[counterName] = val
    }
    data := ran4GHuaDataRow{
        key: key,
        value: valueMap,
    }
    // data := putOrUpdate(key,valueMap)
    fmt.Println("key -- ",key)
    fmt.Println("value -- ",valueMap)
    counterRow := make([]interface{}, len(data.key)+len(counterHeaderList))
    idList := make(map[string]int, len(data.key))
    copy(counterRow,data.key[:])
    for k,v := range data.value {
        counterRow[len(data.key)+counterHeaderIndexMap[k]] = v
    }

    for k,v := range ran4GHuaIDList {
        idList[v] = k
    }

    // declare for parquet struct
    var valColList []string
    var idColList []string
    var parquetDataType []string
    headerToIndex := make(map[string]int)

    dataType := getCounterDataType(counterHeaderList)
    for index, rowSchema := range dataType {
        name, _ := rowSchema["name"]
        typeName, _ := rowSchema["type"]

        parquetDataTypeVal := fmt.Sprintf("name=%s, type=%s", name, typeName)
        if typeName == "DOUBLE" {
			valColList = append(valColList, name)
		} else if typeName == "INT64" {
			parquetDataTypeVal += ", encoding=PLAIN_DICTIONARY"
		} else if typeName == "UTF8" {
			idColList = append(idColList, name)
		}
		headerToIndex[name] = index
		parquetDataType = append(parquetDataType, parquetDataTypeVal) 
    }

    
}

func putOrUpdate(key [len(ran4GHuaIDList)]interface{}, value map[string]float64)map[[len(ran4GHuaIDList)]interface{}]map[string]float64{
    data := make(map[[len(ran4GHuaIDList)]interface{}]map[string]float64)
    if val, ok := data[key]; ok {
        for k,v := range value {
            val[k] = v
        }
        data[key] = val
    } else {
        data[key] = value
    }
    // for k,v := range data {
    //     fmt.Println("key--- ",k)
    //     fmt.Println("value --- ",v)
    // }
    // fmt.Println()
    return data
}

func readCounterList() []string {
    file,_ := os.Open("counterList.txt")
    scanner := bufio.NewScanner(file)
    var counterList []string

    for scanner.Scan() {
        counter := scanner.Text()
        counter = NeutralizeName(counter)
        counterList = append(counterList,counter)
    }
    return counterList
}

func getCounterDataType(counterHeaderList []string) []map[string]string {
	var retVal []map[string]string
	for _, colName := range ran4GHuaIDList {
		var typeName string
		if colName == ran4GHuaTimeColumn1 || colName == ran4GHuaTimeColumn2 {
			typeName = "INT64"
		} else {
			typeName = "UTF8"
		}
		retVal = append(retVal, map[string]string{
			"name": colName,
			"type": typeName,
		})
	}
	for _, colName := range counterHeaderList {
		retVal = append(retVal, map[string]string{
			"name": colName,
			"type": "DOUBLE",
		})
	}
	return retVal
}
