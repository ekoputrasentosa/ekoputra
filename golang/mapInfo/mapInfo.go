package main

import (
    "fmt"
    "time"
    "math/rand"
    "strings"
)


var supportedMapTypeList = map[string]map[string][]string{
	"5G_RAN_HUA_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_hw_enodeb",
		},
		"lookup_key_1": {
			"enodebfunctionname",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
			"bandtype",
			"bandwith",
		},
	},
	"4G_RAN_HUA_2": {
		"table_name": { // table_name can only contain one entry
			"bigdata_hw_enodeb",
		},
		"lookup_key_1": {
			"cell_name", // using cell_name to get more specific mapping
		},
		"id": {
			"cell_name", // apparently also needs to go a level deeper, previously enodebid
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
			"cell_name",
			"bandtype",
			"bandwith",
		},
	},
	"4G_RAN_HUA_USO_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_hw_enodeb",
		},
		"lookup_key_1": {
			"enodebfunctionname",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
		},
	},
	"4G_RAN_HUA_USO_2": {
		"table_name": { // table_name can only contain one entry
			"bigdata_hw_enodeb",
		},
		"lookup_key_1": {
			"enodebfunctionname",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
		},
	},
	"4G_RAN_ERI_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_er_enodeb",
		},
		"lookup_key_1": {
			"enodebid",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
			"bandtype",
			"bandwith",
		},
	},
	"5G_RAN_ERI_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_er_enodeb",
		},
		"lookup_key_1": {
			"enodebid",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
			"bandtype",
			"bandwith",
		},
	},
	"4G_RAN_NOK_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_nok_enodeb",
		},
		"lookup_key_1": {
			"enodebid",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
		},
	},
	"5G_RAN_ZTE_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_enodeb",
		},
		"lookup_key_1": {
			"enodebid",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
		},
	},
	"4G_RAN_ZTE_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_enodeb",
		},
		"lookup_key_1": {
			"enodebid",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
			"bandtype",
			"bandwith",
		},
	},
	"4G_RAN_ZTE_2": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_enodeb",
		},
		"lookup_key_1": {
			"enodebid",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
			"bandtype",
			"bandwith",
		},
	},
	"4G_RAN_ZTE_USO_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_enodeb",
		},
		"lookup_key_1": {
			"enodebid",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
		},
	},
	"4G_RAN_ZTE_USO_2": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_enodeb",
		},
		"lookup_key_1": {
			"enodebid",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
			"regional",
		},
	},
	"3G_RAN_HUA_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_hw_ucell",
		},
		"lookup_key_1": {
			"rnc_name",
			"cell_name",
			"sac",
		},
		"lookup_key_2": {
			"node_b_name",
			"locell",
		},
		"id": {
			"mo_id",
		},
		"values": {
			"mo_id",
			"rnc_name",
			"node_b_name",
			"cell_name",
			"lac",
			"ci",
			"sac",
			"cellid",
			"longitude",
			"latitude",
			"locell",
			"maxtxpower",
			"maxhsdpausernum",
			"maxhsupausernum",
		},
	},
	"3G_RAN_HUA_1_NODEB": {
		"table_name": { // table_name can only contain one entry
			"bigdata_hw_nodeb",
		},
		"lookup_key_1": {
			"node_b_name",
		},
		"id": {
			"mo_id",
		},
		"values": {
			"mo_id",
			"node_b_name",
			"txbw",
			"rxbw",
		},
	},
	"3G_RAN_ERI_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_er_ucell",
		},
		"lookup_key_1": {
			"mo_id",
		},
		"id": {
			"mo_id",
		},
		"values": {
			"mo_id",
			"rnc_name",
			"node_b_name",
			"cell_name",
			"lac",
			"ci",
			"sac",
			"cellid",
			"longitude",
			"latitude",
			"maximumtransmissionpower",
		},
	},
	"3G_RAN_ERI_1_NODEB": {
		"table_name": { // table_name can only contain one entry
			"bigdata_er_nodeb",
		},
		"lookup_key_1": {
			"node_b_name",
		},
		"id": {
			"mo_id",
		},
		"values": {
			"mo_id",
			"node_b_name",
			"userplanegbradmbandwidthdl",
			"userplanegbradmbandwidthul",
		},
	},
	"3G_RAN_NOK_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_nok_ucell",
		},
		"lookup_key_1": {
			"mo_id",
		},
		"id": {
			"mo_id",
		},
		"values": {
			"mo_id",
			"rnc_name",
			"node_b_name",
			"cell_name",
			"lac",
			"ci",
			"sac",
			"cellid",
			"longitude",
			"latitude",
			"maxdlpowercapability",
		},
	},
	"3G_RAN_ZTE_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_ucell",
		},
		"lookup_key_1": {
			"cell_name",
		},
		"id": {
			"mo_id",
		},
		"values": {
			"mo_id",
			"rnc_name",
			"node_b_name",
			"cell_name",
			"lac",
			"ci",
			"sac",
			"cellid",
			"longitude",
			"latitude",
			"maximumtransmissionpower",
		},
	},
	"3G_RAN_ZTE_2": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_ucell",
		},
		"lookup_key_1": {
			"cell_name",
		},
		"id": {
			"cell_name",
		},
		"values": {
			"mo_id",
			"rnc_name",
			"node_b_name",
			"cell_name",
			"lac",
			"ci",
			"sac",
			"cellid",
			"longitude",
			"latitude",
			"maximumtransmissionpower",
		},
	},
	"2G_RAN_HUA_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_hw_g_cell",
		},
		"lookup_key_1": {
			"bsc_name",
			"cell_name",
			"cellid",
		},
		"id": {
			"lac",
			"ci",
		},
		"values": {
			"mo_id",
			"bsc_name",
			"site_name",
			"cell_name",
			"lac",
			"ci",
			"cellid",
			"longitude",
			"latitude",
		},
	},
	"2G_RAN_HUA_USO_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_hw_g_cell",
		},
		"lookup_key_1": {
			"bsc_name",
			"cell_name",
			"cellid",
		},
		"id": {
			"lac",
			"ci",
		},
		"values": {
			"mo_id",
			"bsc_name",
			"site_name",
			"cell_name",
			"lac",
			"ci",
			"cellid",
			"longitude",
			"latitude",
		},
	},
	"2G_RAN_HUA_USO_2": {
		"table_name": { // table_name can only contain one entry
			"bigdata_hw_g_cell",
		},
		"lookup_key_1": {
			"bsc_name",
			"cell_name",
			"cellid",
		},
		"id": {
			"lac",
			"ci",
		},
		"values": {
			"mo_id",
			"bsc_name",
			"site_name",
			"cell_name",
			"lac",
			"ci",
			"cellid",
			"longitude",
			"latitude",
		},
	},
	"2G_RAN_ERI_1": {
		"table_name": {
			"bigdata_er_g_cell",
		},
		"lookup_key_1": {
			"cell_name",
		},
		"id": { // ID can only contain one entry
			"mo_id",
		},
		"values": {
			"mo_id",
			"bsc_name",
			"site_name",
			"cell_name",
			"lac",
			"ci",
			"longitude",
			"latitude",
			"cellid",
		},
	},
	"2G_RAN_NOK_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_nok_g_cell",
		},
		"lookup_key_1": {
			"mo_id",
		},
		"id": {
			"mo_id",
		},
		"values": {
			"mo_id",
			"bsc_name",
			"site_name",
			"cell_name",
			"lac",
			"ci",
			"cellid",
			"longitude",
			"latitude",
			"channel_count",
		},
	},
	"2G_RAN_ZTE_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_g_cell",
		},
		"lookup_key_1": {
			"lac",
			"ci",
		},
		"id": {
			"mo_id",
		},
		"values": {
			"mo_id",
			"bsc_name",
			"site_name",
			"cell_name",
			"lac",
			"ci",
			"cellid",
			"longitude",
			"latitude",
		},
	},

	"2G_RAN_ZTE_USO_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_g_cell",
		},
		"lookup_key_1": {
			"lac",
			"ci",
		},
		"id": {
			"mo_id",
		},
		"values": {
			"mo_id",
			"bsc_name",
			"site_name",
			"cell_name",
			"lac",
			"ci",
			"cellid",
			"longitude",
			"latitude",
		},
	},
	"2G_RAN_ZTE_2": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_g_cell",
		},
		"lookup_key_1": {
			"lac",
			"ci",
		},
		"lookup_key_2": {
			"cell_name",
		},
		"id": {
			"cell_name",
		},
		"values": {
			"mo_id",
			"bsc_name",
			"site_name",
			"cell_name",
			"lac",
			"ci",
			"cellid",
			"longitude",
			"latitude",
		},
	},
	"TRANSPORT_HUA_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_hw_bts",
		},
		"lookup_key_1": {
			"name",
		},
		"lookup_key_2": {
			"mo_id",
			"ani",
		},
		"id": {
			"name",
			// "ani",
		},
		"values": {
			"mo_id",
			"name",
			"ne_id",
			"ani",
		},
	},
	"TRANSPORT_ERI_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_er_nodeb",
		},
		"lookup_key_1": {
			"node_b_name",
		},
		"lookup_key_2": {
			// "ne_id",
			"rbsiubid",
		},
		"id": {
			"node_b_name",
		},
		"values": {
			"node_b_name",
			// "ne_id",
			"rbsiubid",
		},
	},
	"NAURA_ZTE_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_enodeb",
		},
		"lookup_key_1": {
			"enodebid",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
		},
	},
	"4G_VQI_ZTE_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_enodeb",
		},
		"lookup_key_1": {
			"enodebid",
		},
		"id": {
			"enodebid",
		},
		"values": {
			"enodebfunctionname",
			"enodebid",
		},
	},
	"3G_VQI_ZTE_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_ucell",
		},
		"lookup_key_1": {
			"cell_name",
		},
		"id": {
			"cell_name",
		},
		"values": {
			"rnc_name",
			"node_b_name",
			"cell_name",
			"lac",
			"ci",
			"sac",
			"cellid",
			"longitude",
			"latitude",
		},
	},
	"2G_VQI_ZTE_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_g_cell",
		},
		"lookup_key_1": {
			"lac",
			"ci",
		},
		"lookup_key_2": {
			"cell_name",
		},
		"id": {
			"cell_name",
		},
		"values": {
			"bsc_name",
			"site_name",
			"cell_name",
			"lac",
			"ci",
			"cellid",
			"longitude",
			"latitude",
		},
	},
	"SHI_CM_INVENTORY_HWSW_BTS_1": {
		"table_name": { // table_name can only contain one entry
			"bigdata_zte_enodeb",
		},
		"lookup_key_1": {
			"enodebfunctionname",
		},
		"id": {
			"enodebfunctionname",
		},
		"values": {
			"enodebfunctionname",
			"regional",
			"departement",
			"technical_area",
		},
	},
}
var supportedVendorList = []string{
	"ZTE",
	"HUA_1",
	"HUA_2",
	"ZTE_USO",
	"TITAN_1",
}

var supportedMapTypeListSimple = []string{
	"TRANSPORT_ZTE_1",
	"TRANSPORT_NOK_1",
	"CORE_DRA_1",
	"CORE_HLR_1",
	"CORE_HSS_1",
	"CORE_HSS_CAP_1",
	"CORE_DSP_1",
	"CORE_OSSERA_1",
	"CORE_SCP_1",
	"CORE_SCP_UANGEL_1",
	"CORE_DNS_GI_1",
	"CORE_F5_1",
	"CORE_DNS_GN_1",
	"CORE_MICS_1",
	"CORE_MICS_RES_1",
	"CORE_UPCC_1",
	"MQA_1",
	"TRAFFICA_1",
	"TRAFFICA_PS_2",
	"TRAFFICA_CS_2",
	"VAS_MMSC_1",
	"VAS_SMSC_1",
	"AT_HUA_1",
	"AT_HUA_PROV_1",
	"AT_NOK_MICS_1",
	"TWAMP_ZTE_1",
	"TWAMP_HUA_1",
	"TWAMP_ERI_1",
	"TWAMP_NOK_1",
	"TWAMP_BRIX_1",
	"GEOLOC_1",
	"DCO_1",
	"ERAO_BTS_1",
	"ERAO_ZTE_2G_ABIS_1",
	"ERAO_ZTE_2G_AINT_1",
	"ERAO_ZTE_2G_GB_1",
	"ERAO_ZTE_3G_IUB_1",
	"ERAO_ZTE_3G_IUCS_1",
	"ERAO_ZTE_3G_IUPS_1",
	"ERAO_ZTE_4G_1",
	"ERAO_HUA_2G_ABIS_1",
	"ERAO_HUA_2G_AINT_1",
	"ERAO_HUA_2G_BTS_1",
	"ERAO_HUA_2G_GB_1",
	"ERAO_HUA_3G_ENODEB_1",
	"ERAO_HUA_3G_IUB_1",
	"ERAO_HUA_3G_IUCS_1",
	"ERAO_HUA_3G_IUPS_1",
	"ERAO_HUA_4G_1",
	"LACIMA_1",
	"3G_RAN_KPI_1",
	"4G_RAN_KPI_1",
	"CORE_PS_HUA_1",
	"CORE_CS_ERI_1",
	"CORE_PS_ERI_1",
	"CORE_CS_ERI_2",
	"CORE_PS_ERI_2",
	"OSSERA_LINK_1",
	"OSSERA_KPI_1",
	"OSSERA_LINKREPORT_1",
	"OSSERA_PORT_1",
	"OSSERA_DEVICE_1",
	"OSSERA_DEVICE_2",
	"OSSERA_MEMORY_1",
	"OSSERA_RAN_1",
	"OSSERA_METRO_1",
	"OSSERA_GPON_1",
	"OSSERA_CARDS_1",
	"OSSERA_VRF_1",
	"OSSERA_QOS_1",
	"OSSERA_LV_BH_4G_1",
	"OSSERA_QC_4G_1",
	"OSSERA_RAW_4G_1",
	"OSSERA_RAW_3G_1",
	"CHRONO_BIGDATA_HUA_UCELL_1",
	"CHRONO_BIGDATA_HUA_NODEB_1",
	"CHRONO_BIGDATA_ZTE_ENODEB_1",
	"CHRONO_BIGDATA_NOK_ENODEB_1",
	"CHRONO_BIGDATA_HUA_ENODEB_1",
	"CHRONO_BIGDATA_ERI_ENODEB_1",
	"CHRONO_BIGDATA_ERI_NODEB_1",
	"CHRONO_BIGDATA_ERI_UCELL_1",
	"CHRONO_BIGDATA_NOK_UCELL_1",
	"CHRONO_BIGDATA_ZTE_UCELL_1",
	"CHRONO_BIGDATA_ERI_GCELL_1",
	"CHRONO_BIGDATA_HUA_GCELL_1",
	"CHRONO_BIGDATA_NOK_GCELL_1",
	"CHRONO_BIGDATA_ZTE_GCELL_1",
	"CHRONO_BIGDATA_HUA_BTS_1",
	"CHRONO_BIGDATA_NOK_GTRX_1",
	"CHRONO_LACIMA_LONGLAT_1",
	"CHRONO_5G_LACIMA_ENR_1",
	"CHRONO_5G_LACIMA_1",
	"CHRONO_4G_LACIMA_1",
	"CHRONO_4G_LACIMA_ENR_1",
	"CHRONO_AUDITLONGLAT_1",
	"CHRONO_3G_LACIMA_1",
	"CHRONO_3G_LACIMA_ENR_1",
	"CHRONO_2G_LACIMA_1",
	"CHRONO_2G_LACIMA_ENR_1",
	"CHRONO_4G_LACIMA_USO_1",
	"CHRONO_2G_LACIMA_USO_1",
	"CHRONO_4G_LACIMA_USO_ENR_1",
	"CHRONO_2G_LACIMA_USO_ENR_1",
	"CHRONO_NSTO_LIST_ENODEB_1",
	"CHRONO_4G_NSTO_1",
	"CHRONO_2G_NSTO_USO_1",
	"CHRONO_5G_BASELINE_1",
	"CHRONO_4G_BASELINE_1",
	"CHRONO_3G_BASELINE_1",
	"CHRONO_2G_BASELINE_1",
	"CHRONO_2G_BASELINE_USO_1",
	"CHRONO_4G_BASELINE_USO_1",
	"CHRONO_5G_DAILYSYSINFO_1",
	"CHRONO_4G_DAILYSYSINFO_1",
	"CHRONO_3G_DAILYSYSINFO_1",
	"CHRONO_2G_DAILYSYSINFO_1",
	"CHRONO_4G_DAILYSYSINFO_USO_1",
	"CHRONO_2G_DAILYSYSINFO_USO_1",
	"CHRONO_3G_ANI_1",
	"CHRONO_2G_NSTO_1",
	"CHRONO_4G_NSTO_USO_1",
	"CHRONO_3G_NSTO_1",
	"ISD_1",
	"TA_ERI_1",
	"SBC_1",
	"EIR_1",
	"SBC_2",
	"B2B_APN_CORPORATE_1",
	"B2B_APN_ALARM_1",
	"SPEEDTEST_SPEED_1",
	"SPEEDTEST_COVERAGE_1",
	"GRAFANA_1",
	"SPEEDTEST_CSENTIMEN_1",
	"NIO_SMY_BCP_CATEGORISATION_DD_1",
	"NIO_SMY_BCP_USAGE_DD_1",
	"NIO_SMY_BCP_USAGE_HH_1",
	"NIO_V_ABT_STG_BCP_USAGE_DD_1",
	"NIO_V_ABT_SUMM_BCP_USAGE_DD_1",
	"NIO_V_SMY_BCP_CATEGORISATION_DD_1",
	"NIO_V_ABT_STG_BCP_USAGE_MM_1",
	"NIO_V_ABT_SUMM_BCP_USAGE_MM_1",
	"NIO_V_BCPUSG_APPSCAT_APPS_COMP_1",
	"NIO_V_STG_ABT_BCP_APP_1",
	"NIO_V_STG_ABT_BCP_CAT_1",
	"NIO_V_STG_ABT_BCP_SCORE_APP_1",
	"NIO_V_STG_ABT_BCP_SCORE_CAT_1",
	"SMARTCARE_USERPLANE_FILEACCESS_1",
	"SMARTCARE_USERPLANE_VOIP_1",
	"SMARTCARE_USERPLANE_IM_1",
	"SMARTCARE_USERPLANE_WEB_1",
	"SMARTCARE_USERPLANE_STREAM_1",
	"NVS_TITAN_ENUM_1",
	"NVS_SBC_P_CSCF_1",
	"NVS_S_CSCF_1",
	"NVS_NTAS_1",
	"REDCELL_1",
	"REDCELL_TA_HUA_1",
	"REDCELL_TA_NOK_1",
	"REDCELL_TA_ERI_1",
	"REDCELL_TA_ZTE_1",
	"SMARTCARE_SDR_DYN_REGION_HTTP_SR_1",
	"SMARTCARE_SDR_DYN_NATIONAL_HTTP_SR_1",
	"SMARTCARE_SDR_DYN_REGION_ALL_IP_1",
	"RAN_KPI_ENR_1",
	"PRB_AVAIL_HUA_1",
	"CORE_REALTIME_CMM_NOKIA_1",
	"CORE_REALTIME_NOKIA_CS_1",
	"CORE_NEAR_REALTIME_HUA_1",
	"CORE_REALTIME_CS_ERI_1",
	"CORE_REALTIME_PS_ERI_1",
	"CORE_REALTIME_FNS_NOKIA_1",
	"CORE_REALTIME_NOKIA_CS_2",
	"CORE_GGSN_ERI_NDW_1",
	"CORE_SGSN_ERI_NDW_1",
	"CORE_MSS_ERI_NDW_1",
	"CORE_MGW_ERI_NDW_1",
	"CORE_5G_ERI_GGSN_1",
	"CORE_5G_ERI_SGSN_1",
	"MICS_DB_COMPANY_1",
	"MICS_DB_USER_1",
	"MICS_DB_USERNUMBER_1",
	"OSSERA_ATOM_1",
	"OSSERA_VGGSN_1",
	"OSSERA_CNOP_1",
	"OSSERA_RAW_GINET_1",
	"OSSERA_PSGI_1",
	"OSSERA_IX_1",
	"OSSERA_IX_2",
	"WTTX_ORBIT_1",
	"WTTX_MSISDN_REFERENCE_1",
	"CORE_REALTIME_GGSN_ERI_1",
	"DATACOMM_AVAILABILITY_1",
	"DATACOMM_BACKHAUL_1",
	"OSSERA_IPWAN_1",
	"OSSERA_IPRAN_1",
	"SPS_M3UA_1",
	"SPS_LICENSE_1",
	"SPS_M2PA_1",
	"IMS_CSCF_1",
	"PAGING_2G_BSC_1",
	"XPU_DSP_6910_BSC_1",
	"SHI_CM_INVENTORY_HWSW_CONTROLLER_1",
	"SHI_CM_INVENTORY_HWSW_BTS_1",
	"SHI_CM_INVENTORY_LICENSE_BTS_CONTROLLER_1",
	"SHI_CM_INVENTORY_LICENSE_BTS_CONTROLLER_2",
	"SHI_CONFIGURATION_EAS_BTS_EMS_1",
	"SHI_CONFIGURATION_EAS_BTS_UME_1",
	"SHI_CONFIGURATION_EAS_CONTROLLER_EMS_1",
	"SHI_CONFIGURATION_EAS_CONTROLLER_UME_1",
	"SHI_REFERENCE_CONTROLLER_1",
	"SHI_MACADDRESS_1",
    "SHI_MACADDRESS_CONTROLLER_1",
	"SHI_ALARM_CODES_1",
	"SHI_ALARM_CODES_2",
	"SHI_EID_EOS_LIFE_CYCLE_SOFTWARE_1",
	"SHI_EID_EOS_LIFE_CYCLE_HARDWARE_1",
	"SHI_ZTE_EOS_LIFE_CYCLE_SOFTWARE_1",
	"SHI_ZTE_EOS_LIFE_CYCLE_1",
	"SHI_ZTE_EOS_LIFE_CYCLE_2",
	"SHI_HWI_EOS_LIFE_CYCLE_HARDWARE_1",
	"SHI_HWI_EOS_LIFE_CYCLE_SOFTWARE_1",
	"SHI_ZTE_STATUS_MODUL_1",
	"SHI_ZTE_CPU_LOAD_1",
	"SHI_ZTE_PAGING_LOAD_1",
	"SHI_ZTE_UTILISASI_1",
	"SHI_ZTE_DATA_INTERFACE_1",
	"SHI_ZTE_POWER_CONSUMPTION_BTS_1",
	"SHI_ERI_CPU_LOAD_SITE_LEVEL_1",
	"SHI_ERI_CPU_LOAD_LEVEL_CONTROLLER_1",
	"SHI_ERI_INVENTORY_HW_CONTROLLER_1",
	"SHI_ERI_INVENTORY_HW_BTS_1",
	"SHI_ERI_UTILISASI_IUPS_IUCS_1",
	"SHI_ERI_INVENTORY_LICENSE_FEATURE_PARAMETER_BTS_1",
    "SHI_ERI_INVENTORY_LICENSE_FEATURE_PARAMETER_CONTROLLER_3G_RNC_1",
    "SHI_ERI_EAS_SITE_BTS_1",
    "SHI_ERI_EAS_CONTROLLER_BSC_1",
    "SHI_ERI_UTILISASI_IUB_S1_4G_S1_5G_1",
	"SHI_CM_INVENTORY_1",
	"SHI_LICENCE_1",
	"SHI_PARAMETER_1",
	"SHI_VLAN_1",
	"SHI_VLAN_2",
	"SHI_VLAN_BTS_1",
	"SHI_NE_RAN_EXPORT_1",
	"SHI_PM_EXPORT_1",
	"FDI_ERICSSON_1",
	"FDI_HUAWEI_1",
	"FDI_ZTE_1",
	"FDI_USER_ALARM_CORE_1",
	"TA_ERI_2",
	"CORE_GGSN_HUA_1",
	"CORE_SGSN_HUA_1",
	"CORE_5G_HUAWEI_1",
	"CORE_MSS_NOK_1",
	"CORE_MGW_NOK_1",
	"AT_RADIO_HUA_SECURITY_1",
	"AT_RADIO_HUA_SYSTEM_1",
	"AT_RADIO_HUA_OPERATION_1",
	"AT_RADIO_HUA_SECURITY_2",
	"AT_RADIO_HUA_SYSTEM_2",
	"AT_RADIO_HUA_OPERATION_2",
	"TRAFFIC_TRANSPORT_IMS_1",
	"TRAFFIC_TRANSPORT_IMS_2",
	"CH_FEGE_BSC_1",
	"SHT_IUB_IP_1",
	"IMS_TRANSPORT_5G_1",
	"DSP_6900_BSC_1",
	"XPU_6900_BSC_1",
}

type MapInfo struct{
    registeredType  map[string]bool
}

func (info *MapInfo) cellPrepareSQLStatement(etlType string) string {
	tableName := supportedMapTypeList[etlType]["table_name"][0]
	var sqlStatement string
	sqlStatement += fmt.Sprintf("SELECT %s ", strings.Join(supportedMapTypeList[etlType]["values"], ", "))
	sqlStatement += fmt.Sprintf("FROM %s ", tableName)
	if etlType == "2G_RAN_NOK_1" {
		sqlStatement += fmt.Sprintf(`
LEFT JOIN (
	SELECT SPLIT_PART(MO_ID, '/', 4) AS mo_id_raw,
           (SUM(CHANNEL0TYPE_COUNT) + SUM(CHANNEL1TYPE_COUNT) + SUM(CHANNEL2TYPE_COUNT) + SUM(CHANNEL3TYPE_COUNT) + SUM(CHANNEL4TYPE_COUNT) + SUM(CHANNEL5TYPE_COUNT) + SUM(CHANNEL6TYPE_COUNT) + SUM(CHANNEL7TYPE_COUNT)) AS channel_count
	FROM
	  (SELECT MO_ID,
	          CASE
	              WHEN CHANNEL0TYPE IN ('0',
	                                    '1',
	                                    '2') THEN 1
	              ELSE 0
	          END AS CHANNEL0TYPE_COUNT,
	          CASE
	              WHEN CHANNEL1TYPE IN ('0',
	                                    '1',
	                                    '2') THEN 1
	              ELSE 0
	          END AS CHANNEL1TYPE_COUNT,
	          CASE
	              WHEN CHANNEL2TYPE IN ('0',
	                                    '1',
	                                    '2') THEN 1
	              ELSE 0
	          END AS CHANNEL2TYPE_COUNT,
	          CASE
	              WHEN CHANNEL3TYPE IN ('0',
	                                    '1',
	                                    '2') THEN 1
	              ELSE 0
	          END AS CHANNEL3TYPE_COUNT,
	          CASE
	              WHEN CHANNEL4TYPE IN ('0',
	                                    '1',
	                                    '2') THEN 1
	              ELSE 0
	          END AS CHANNEL4TYPE_COUNT,
	          CASE
	              WHEN CHANNEL5TYPE IN ('0',
	                                    '1',
	                                    '2') THEN 1
	              ELSE 0
	          END AS CHANNEL5TYPE_COUNT,
	          CASE
	              WHEN CHANNEL6TYPE IN ('0',
	                                    '1',
	                                    '2') THEN 1
	              ELSE 0
	          END AS CHANNEL6TYPE_COUNT,
	          CASE
	              WHEN CHANNEL7TYPE IN ('0',
	                                    '1',
	                                    '2') THEN 1
	              ELSE 0
	          END AS CHANNEL7TYPE_COUNT
	   FROM bigdata_nok_gtrx) CHANNEL_DATA
	GROUP BY SPLIT_PART(MO_ID, '/', 4)
) B ON (B.mo_id_raw = SPLIT_PART(%s.MO_ID, '/', 4)) `, tableName)
	}
	if etlType == "TRANSPORT_HUA_1" {
		sqlStatement = fmt.Sprintf(`(SELECT '' as mo_id, name, ne_id, ani
		FROM bigdata_hw_bts)
		UNION ALL
		(SELECT mo_id, node_b_name as name, ne_id, ani
		FROM bigdata_hw_nodeb)`)
	}
	if etlType == "5G_RAN_HUA_1" || etlType == "5G_RAN_ERI_1" || etlType == "4G_RAN_ERI_1" {
		sqlStatement = fmt.Sprintf(`SELECT enodebfunctionname, %s.enodebid, regional,
	    split_part(B.bandtype, ' ', 2) as bandtype, B.bandwith as bandwith
	    FROM %s
	    LEFT JOIN (
	        SELECT bandtype, bandwith, enodebid
	        FROM baseline_4g
	    ) B ON B.enodebid = %s.enodebid
`, tableName, tableName, tableName)
	}
	if etlType == "4G_RAN_HUA_2" { // separated since containing 6 values (rows)
		sqlStatement = fmt.Sprintf(`SELECT enodebfunctionname, %s.enodebid, regional, B.cell_name,
	    split_part(B.bandtype, ' ', 2) as bandtype, B.bandwith as bandwith
	    FROM %s
	    LEFT JOIN (
	        SELECT bandtype, bandwith, enodebid, cell_name
	        FROM baseline_4g
	    ) B ON B.enodebid = %s.enodebid
`, tableName, tableName, tableName) // to also include cell_name for more precise mapping
	}
	if etlType == "SHI_CM_INVENTORY_HWSW_BTS_1" { // separated since containing 4 values (rows)
		sqlStatement = fmt.Sprintf(`SELECT enodebfunctionname, regional, B.departement as departement,
	    B.technical_area as technical_area
	    FROM %s
	    LEFT JOIN (
	        SELECT departement, technical_area, nodeb_name
	        FROM baseline_4g
	    ) B ON B.nodeb_name = %s.enodebfunctionname
`, tableName, tableName) // to also include cell_name for more precise mapping
	}
	if etlType == "2G_RAN_ZTE_1" ||
		etlType == "2G_RAN_ZTE_USO_1" ||
		etlType == "2G_RAN_ZTE_2" ||
		etlType == "2G_VQI_ZTE_1" {
		sqlStatement = fmt.Sprintf("SELECT A.%s ", strings.Join(supportedMapTypeList[etlType]["values"], ", A."))
		sqlStatement += fmt.Sprintf(`FROM ( SELECT %s, row_number()over(partition by cell_name order by "timestamp" desc) as rn
					     FROM %s
					     GROUP BY %s, "timestamp" ) as A
					     `, strings.Join(supportedMapTypeList[etlType]["values"], ", "), tableName, strings.Join(supportedMapTypeList[etlType]["values"], ", "))
		sqlStatement += fmt.Sprintf("WHERE A.rn = 1")
	}
	if etlType == "3G_RAN_ZTE_1" ||
		etlType == "3G_RAN_ZTE_2" ||
		etlType == "3G_VQI_ZTE_1" {
		sqlStatement = fmt.Sprintf("SELECT A.%s ", strings.Join(supportedMapTypeList[etlType]["values"], ", A."))
		sqlStatement += fmt.Sprintf(`FROM ( SELECT %s, row_number()over(partition by cell_name order by "timestamp" desc) as rn
					     FROM %s
					     GROUP BY %s, "timestamp" ) as A
					     `, strings.Join(supportedMapTypeList[etlType]["values"], ", "), tableName, strings.Join(supportedMapTypeList[etlType]["values"], ", "))
		sqlStatement += fmt.Sprintf("WHERE A.rn = 1")
	}
	if etlType == "4G_RAN_ZTE_1" ||
		etlType == "4G_RAN_ZTE_2" {
		sqlStatement = fmt.Sprintf(`SELECT A.enodebfunctionname, A.enodebid, A.regional,
		split_part(B.bandtype, ' ', 2) as bandtype, B.bandwith as bandwith
		FROM ( SELECT enodebfunctionname, enodebid, regional,
					row_number()over(partition by enodebid order by id desc) as rn
				FROM %s
			) A
		LEFT JOIN (
			SELECT bandtype, bandwith, enodebid,
				row_number()over(partition by enodebid order by id desc) as rn
			FROM baseline_4g
		) B ON B.enodebid = A.enodebid
		WHERE B.rn = 1 AND A.rn = 1
`, tableName)
	}
	if etlType == "5G_RAN_ZTE_1" ||
		etlType == "4G_RAN_ZTE_USO_1" ||
		etlType == "4G_RAN_ZTE_USO_2" ||
		etlType == "NAURA_ZTE_1" ||
		etlType == "4G_VQI_ZTE_1" {
		sqlStatement = fmt.Sprintf("SELECT A.%s ", strings.Join(supportedMapTypeList[etlType]["values"], ", A."))
		sqlStatement += fmt.Sprintf(`FROM ( SELECT %s, row_number()over(partition by enodebid order by id desc) as rn
					     FROM %s ) as A
					     `, strings.Join(supportedMapTypeList[etlType]["values"], ", "), tableName)
		sqlStatement += fmt.Sprintf("WHERE A.rn = 1")
	}
	return sqlStatement
}

func (info *MapInfo) cellLoadData(etlType string) {
    headerToIndex := make(map[string]int)
    for index, header := range supportedMapTypeList[etlType]["values"]{
        headerToIndex[header] = index
    }
    sqlStatement := info.cellPrepareSQLStatement(etlType)
    fmt.Println(sqlStatement)
    
    lookupKeyCols1 := supportedMapTypeList[etlType]["lookup_key_1"]
    for _,lookupKeyVal := range lookupKeyCols1{
        value := headerToIndex[lookupKeyVal]
        fmt.Println(value)
    }
}

var autoLoadDuration = time.Hour*3 + time.Duration(rand.Intn(900))*time.Second

func main() {
    info := MapInfo{}
    for etlType := range supportedMapTypeList{
        fmt.Println(etlType)
        fmt.Println("############ cellLoadData ############")
        info.cellLoadData(etlType)
    }
}
