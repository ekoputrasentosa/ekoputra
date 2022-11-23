
class Huawei:
    class scheduler:
        radio_4g = [
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_HW_Cell_Measurement.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_HW_CQI_eNodeB.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_HW_Handover_4G.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_HW_IP_Layer_4G.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_HW_PDCCH_TA.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_HW_Transport_Resource.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_HW_Npsacbp_Wireless_Broadband_wbb.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_Accessibility_Volte.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_Retainability_Volte.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_Mobility_Volte.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_Traffic_Volte.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_Integrity_Volte.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_E2E MOS_Volte.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_HW_Transport_X2.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_Huawei_MCS.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_LOM_CQI.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)SHT_DRD_.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)4G_CellCounter1.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)4G_CellCounter2.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)4G_NB-IoT_Counter.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)KPI_4G_Reguler.*",  # for USO ul_resource_block_utilizing_rate and dl_resource_block_utilizing_rate
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)KPI_Reguler_Packet_Loss.*",  # for USO max_packet_loss and mean_packet_loss
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)PAGING_4G_eNodeB.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)E2E_VQI.*",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)QCI_128.(txt|csv|csv.gz)$",
            r".*(?!.*15_m)(?!.*15m)(?!.*_15)NPSM_WTTX.*",
        ]
