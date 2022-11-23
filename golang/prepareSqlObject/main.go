package main

import (
    "strings"
    "fmt"
    "strconv"
)

var TableIndex = map[string]string{
	"kpi_2g_ran_hourly_new":                   "uix_kpi_2g_ran_hourly_new",
	"kpi_3g_ran_ericsson_hourly_new":          "uix_kpi_3g_ran_ericsson_hourly_new",
	"kpi_3g_ran_huawei_hourly_new":            "uix_kpi_3g_ran_huawei_hourly_new",
	"kpi_3g_ran_nokia_hourly_new":             "uix_kpi_3g_ran_nokia_hourly_new",
	"kpi_3g_ran_zte_hourly_new":               "uix_kpi_3g_ran_zte_hourly_new",
	"kpi_core_dra_hourly":                     "uix_kpi_core_dra_hourly_time_ne_name",
	"kpi_core_scp_hourly":                     "uix_kpi_core_scp_hourly_time_ne_name",
	"kpi_core_scp_uangel_hourly":              "uix_kpi_core_scp_uangel_hourly_time_ne_name",
	"kpi_core_mics_hourly":                    "uix_kpi_core_mics_hourly",
	"rdr_vip_transaction":                     "uix_rdr_vip_transaction_time_end_to_end_id_imsi_peer_ne",
	"kpi_4g_ran_ericsson_hourly_new":          "uix_kpi_4g_ran_ericsson_hourly_new",
	"kpi_4g_ran_zte_hourly_new":               "uix_kpi_4g_ran_zte_hourly_new",
	"kpi_4g_naura_zte_hourly":                 "uix_kpi_4g_naura_zte_hourly",
	"kpi_4g_vqi_zte_hourly":                   "uix_kpi_4g_vqi_zte_hourly",
	"kpi_3g_vqi_zte_hourly":                   "uix_kpi_3g_vqi_zte_hourly",
	"kpi_2g_vqi_zte_hourly":                   "uix_kpi_2g_vqi_zte_hourly",
	"kpi_4g_ran_nokia_hourly_new":             "uix_kpi_4g_ran_nokia_hourly_new",
	"kpi_4g_ran_huawei_hourly_new":            "uix_kpi_4g_ran_huawei_hourly_new",
	"traffica_sai_longcall":                   "uix_id_traffica_sai_longcall",
	"traffica_sai_dropcall":                   "uix_id_traffica_sai_dropcall",
	"geoloc_data":                             "geoloc_data_tile_id_key",
	"kpi_core_ossera_hourly":                  "uix_kpi_core_ossera_hourly",
	"kpi_core_hss_hourly":                     "uix_kpi_core_hss_hourly",
	"kpi_core_hlr_hourly":                     "uix_kpi_core_hlr_hourly",
	"kpi_core_upcc_hourly":                    "uix_kpi_core_upcc_hourly_time_ne_name",
	"kpi_core_dsp_hourly":                     "uix_kpi_core_dsp_hourly",
	"traffica_ps_2g_sgsn_rtt_data":            "uix_traffica_ps_2g_sgsn_rtt_data",
	"traffica_ps_3g_sgsn_rtt_data":            "uix_traffica_ps_3g_sgsn_rtt_data",
	"traffica_ps_fns_data":                    "uix_traffica_ps_fns_data",
	"kpi_core_huawei_ps_hourly_hypertable":    "uix_kpi_core_huawei_ps_hourly_hypertable",
	"kpi_core_ericsson_ps1_hourly_hypertable": "uix_kpi_core_ericsson_ps1_hourly_hypertable",
	"kpi_core_ericsson_cs1_hourly_hypertable": "uix_kpi_core_ericsson_cs1_hourly_hypertable",
	"kpi_core_ericsson_ps2_hourly_hypertable": "uix_kpi_core_ericsson_ps2_hourly_hypertable",
	"kpi_core_ericsson_cs2_hourly_hypertable": "uix_kpi_core_ericsson_cs2_hourly_hypertable",
	"kpi_core_nokia_ps_2g_hourly_hypertable":  "uix_kpi_core_nokia_ps_2g_hourly_hypertable",
	"kpi_core_nokia_ps_3g_hourly_hypertable":  "uix_kpi_core_nokia_ps_3g_hourly_hypertable",
	"kpi_core_nokia_ps_4g_hourly_hypertable":  "uix_kpi_core_nokia_ps_4g_hourly_hypertable",
	"kpi_core_nokia_cs_hourly_hypertable":     "uix_kpi_core_nokia_cs_hourly_hypertable",
	"kpi_core_ossera_hourly_hypertable":       "uix_kpi_core_ossera_hourly_hypertable",
	"kpi_core_hss_hourly_hypertable":          "uix_kpi_core_hss_hourly_hypertable",
	"kpi_core_hlr_hourly_hypertable":          "uix_kpi_core_hlr_hourly_hypertable",
	"kpi_core_upcc_hourly_hypertable":         "uix_kpi_core_upcc_hourly_hypertable_time_ne_name",
	"kpi_core_dsp_hourly_hypertable":          "uix_kpi_core_dsp_hourly_hypertable",
	"bigdata_er_nodeb":                        "uix_bigdata_er_nodeb",
	"bigdata_er_ucell":                        "uix_bigdata_er_ucell",
	"bigdata_nok_ucell":                       "uix_bigdata_nok_ucell",
	"bigdata_zte_ucell":                       "uix_bigdata_zte_ucell",
	"bigdata_er_g_cell":                       "uix_bigdata_er_g_cell",
	"bigdata_hw_enodeb":                       "uix_bigdata_hw_enodeb",
	"bigdata_hw_nodeb":                        "uix_bigdata_hw_nodeb",
	"bigdata_hw_ucell":                        "uix_bigdata_hw_ucell",
	"bigdata_er_enodeb":                       "uix_bigdata_er_enodeb",
	"bigdata_nok_enodeb":                      "uix_bigdata_nok_enodeb",
	"bigdata_zte_enodeb":                      "uix_bigdata_zte_enodeb",
	"bigdata_hw_g_cell":                       "uix_bigdata_hw_g_cell",
	"bigdata_nok_g_cell":                      "uix_bigdata_nok_g_cell",
	"bigdata_zte_g_cell":                      "uix_bigdata_zte_g_cell",
	"bigdata_hw_bts":                          "uix_bigdata_hw_bts",
	"bigdata_nok_gtrx":                        "uix_bigdata_nok_gtrx",
	"baseline_4g":                             "uix_baseline_4g",
	"radio_kpi_enrichment_data":               "uix_radio_kpi_enrichment_data",
	"kpi_smart_sdr_data":                      "uix_kpi_smart_sdr_data",
	"wttx_msisdn_reference":                   "uix_wttx_msisdn_reference_msisdn_imei_esn",
}

func CreatePatternPsql(startIndex int, dataLen int) string {
	pattern := "("
	n := startIndex * dataLen
	for i := 1; i <= dataLen; i++ {
		n++
		pattern += "$" + strconv.Itoa(n) + ", "
	}
	pattern = strings.TrimSuffix(pattern, ", ") + "), "
	return pattern
}

type SQLObject struct {
	Col string
	Set string
}

// Find takes a slice and looks for an element in it. If found it will
// return it's key, otherwise it will return -1 and a bool of false.
func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

// columnNotSetUpdate : set name column not to update, column constraint
func PrepareInsertPsqlSetCol(idList []string, columnNotSetUpdate []string) SQLObject {
	mergeList := idList
	var colString string
	var setString string
	for _, c := range mergeList {
		_, found := Find(columnNotSetUpdate, c)
        // fmt.Println(c,found)
		if !found {
			setString += fmt.Sprintf("%s = EXCLUDED.%s, ", c, c)
		}
		colString += fmt.Sprintf("%s, ", c)
	}
	sqlObject := SQLObject{
		Col: strings.TrimSuffix(colString, ", "),
		Set: strings.TrimSuffix(setString, ", "),
	}
	return sqlObject
}

func main() {
    etlTypeDoNothing := []string{
		"CHRONO_BIGDATA_HUA_BTS_1",
		"WTTX_MSISDN_REFERENCE_1",
	}
	etlTypeOthers := []string{
		"SMARTCARE_SDR_DYN_REGION_HTTP_SR_1",
		"SMARTCARE_SDR_DYN_NATIONAL_HTTP_SR_1",
		"SMARTCARE_SDR_DYN_REGION_ALL_IP_1",
		"CORE_REALTIME_CMM_NOKIA_1",
		"CORE_REALTIME_NOKIA_CS_1",
		"CORE_NEAR_REALTIME_HUA_1",
		"CORE_NEAR_REALTIME_2G_HUA_1",
		"CORE_NEAR_REALTIME_3G_HUA_1",
		"CORE_NEAR_REALTIME_4G_HUA_1",
		"CORE_REALTIME_FNS_NOKIA_1",
		"CORE_REALTIME_NOKIA_CS_2",
		"CORE_REALTIME_PS_ERI_1",
		"SHI_REFERENCE_CONTROLLER_1",
		"SHI_ALARM_CODES_1",
		"SHI_ALARM_CODES_2",
		"SHI_ZTE_EOS_LIFE_CYCLE_SOFTWARE_1",
		"SHI_ZTE_EOS_LIFE_CYCLE_1",
		"SHI_ZTE_EOS_LIFE_CYCLE_2",
		"NVS_S_CSCF_1",
		"NVS_NTAS_1",
		"NVS_SBC_P_CSCF_1",
		"NVS_TITAN_ENUM_1",
		"SHI_MACADDRESS_1",
		"SHI_MACADDRESS_CONTROLLER_1",
	}
    var HwEnodebIDList = [3]string{ // the array size MUST EXACTLY match with its content size
        "enodebfunctionname",
        "enodebid",
        "regional",
    }

    var HwColumnNotSetUpdate = []string{
        "enodebfunctionname",
        "enodebid",
    }

    var dataRows = [][]string{
        {"JBI003M41_Telanaipura-TKM","100003","REGIONAL2"},
        {"JBI005M41_Sriwijaya","100005","REGIONAL2"},
        {"JBI001M41_JambiCentrum-TKM","100001","REGIONAL2"},
        {"JBI012M41_KenaliAsam","100012","REGIONAL2"},
    }

    var vals []interface{}
    etlType := "CHRONO_BIGDATA_HUA_ENODEB_1"
    tableName := "bigdata_hw_enodeb"
    sqlObj := PrepareInsertPsqlSetCol(HwEnodebIDList[:],HwColumnNotSetUpdate) 
    sqlStatement := fmt.Sprintf(`INSERT INTO %s (%s) VALUES `,tableName , sqlObj.Col)

    rowLength := len(HwEnodebIDList)

    for i,row := range dataRows {
        sqlStatement += CreatePatternPsql(i,rowLength)
        idDataTemp := row[:len(HwEnodebIDList)]
        for _,r := range idDataTemp {
            vals = append(vals, r)
        }
    }
    sqlStatement = strings.TrimSuffix(sqlStatement,", ")
    _, found := Find(etlTypeDoNothing, etlType)
	_, foundOthers := Find(etlTypeOthers, etlType)
    
    if found {
		sqlStatement += fmt.Sprintf(` ON CONFLICT ON CONSTRAINT %s DO NOTHING`, TableIndex[tableName])
	} else if foundOthers {
		var constQuery string
		constQuery = strings.Join(HwColumnNotSetUpdate, ",")
		sqlStatement += fmt.Sprintf(` ON CONFLICT (%s) DO UPDATE SET %s`, constQuery, sqlObj.Set)
	} else {
		sqlStatement += fmt.Sprintf(` ON CONFLICT ON CONSTRAINT %s DO UPDATE SET %s`, TableIndex[tableName], sqlObj.Set)
	}
    fmt.Println(sqlStatement)

}
