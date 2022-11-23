package main

import (
	"fmt"
	"strings"
)

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

func NeutralizeSiteName(name string) string {
	for _, prefix := range siteNameReplaceList {
		if strings.HasPrefix(name, prefix) {
			name = strings.Replace(name, prefix, "", 1)
			break
		}
	}
	return name
}

func ParseObjectName5G(objectName string) map[string]string {
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
							propertySplit[0] == "gNodeB Function Name" ||
							propertySplit[0] == "Cell Name" ||
							propertySplit[0] == "NR DU Cell Name" {
							result[propertySplit[0]] = NeutralizeSiteName(propertySplit[1])
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
    objectName := "E_SMG041_CITRALAND/NRDUCELL:gNodeB Function Name=C_SMG041M51_CITRALAND, NR DU Cell ID=93, NR DU Cell Name=C_SMG041MK1_CITRALANDMACRO_MK503, gNodeB ID=5196041, Cell ID=93"
    fmt.Println("raw oject")
    fmt.Println(objectName)
    objectInfo := ParseObjectName5G(objectName)
    fmt.Println(objectInfo)
    for k,v := range objectInfo{
        fmt.Println(k,"-->",v)
    }
}
