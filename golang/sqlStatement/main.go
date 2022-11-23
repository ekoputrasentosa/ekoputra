package main

import (
    "fmt"
    "strings"
)

var supportedMapTypeList = map[string]map[string][]string{
	"5G_RAN_HUA_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_gnodeb",
		},
		"lookup_key_1": {
			"sitename",
		},
		"id": {
			"gnodebid",
			"ci",
		},
		"values": {
			"tac",
			"gnodebid",
			"ci",
			"siteid",
			"neid",
			"sitename",
			"cellname",
			"latitude",
			"longitude",
			"regional",
			"vendor",
		},
	},
    	"5G_RAN_ERI_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_gnodeb",
		},
		"lookup_key_1": {
			"sitename",
		},
		"id": {
			"gnodebid",
			"ci",
		},
		"values": {
			"tac",
			"gnodebid",
			"ci",
			"siteid",
			"neid",
			"sitename",
			"cellname",
			"latitude",
			"longitude",
			"regional",
			"vendor",
		},
	},
    	"5G_RAN_ZTE_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_gnodeb",
		},
		"lookup_key_1": {
			"sitename",
		},
		"id": {
			"gnodebid",
			"ci",
		},
		"values": {
			"tac",
			"gnodebid",
			"ci",
			"siteid",
			"neid",
			"sitename",
			"cellname",
			"latitude",
			"longitude",
			"regional",
			"vendor",
		},
	},
}

func cellPrepareSQLStatement(etlType string) string {
    tableName := supportedMapTypeList[etlType]["table_name"][0]
    var sqlStatement string
		sqlStatement = fmt.Sprintf("SELECT A.%s ", strings.Join(supportedMapTypeList[etlType]["values"], ", A."))
		sqlStatement += fmt.Sprintf(`FROM ( SELECT %s, row_number()over(partition by gnodebid,ci order by id desc) 
						as rn FROM %s ) as A
					     `, strings.Join(supportedMapTypeList[etlType]["values"], ", "), tableName)
		sqlStatement += fmt.Sprintf("WHERE A.rn = 1")
    return sqlStatement
}

func main() {
    for etlType := range supportedMapTypeList {
        sqlStatement := cellPrepareSQLStatement(etlType)
        fmt.Println(sqlStatement)
    }
}
