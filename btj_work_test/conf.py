import datetime
from abc import abstractmethod
from collections import OrderedDict
from typing import List, Tuple, Optional
from dateutil.parser import isoparse

class _TypeBase:
    @property
    @abstractmethod
    def vendor(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def radio(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def plugin(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def file_interval(self) -> datetime.timedelta:
        raise AttributeError

    @property
    @abstractmethod
    def output_file_interval(self) -> datetime.timedelta:
        raise AttributeError

    @property
    @abstractmethod
    def hadoop_output_dir(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def type(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def hive_table_format(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_hadoop_output_dir(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_hive_table(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_daily_hadoop_output_dir(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_daily_hive_table(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_weekly_hadoop_output_dir(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_weekly_hive_table(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_monthly_hadoop_output_dir(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_monthly_hive_table(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_quarterly_hadoop_output_dir(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_quarterly_hive_table(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_hourly_hive_table(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def kpi_hourly_hadoop_output_dir(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def name(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def desc(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def lookback_hours(self) -> List[int]:
        raise AttributeError

    @property
    @abstractmethod
    def run_interval_minutes(self) -> List[int]:
        raise AttributeError

    @property
    @abstractmethod
    def run_at(self) -> List[str]:
        raise AttributeError

    @property
    @abstractmethod
    def source(self) -> str:
        raise AttributeError

    @property
    @abstractmethod
    def hadoop_raw_data_retention_hours(self) -> int:
        raise AttributeError

    @property
    @abstractmethod
    def hadoop_agg_data_retention_hours(self) -> int:
        raise AttributeError

    @property
    @abstractmethod
    def agg_list(self) -> List[str]:
        return []

    @property
    @abstractmethod
    def schema_list(self) -> List[str]:
        return []

    @property
    @abstractmethod
    def aggregate_process(self) -> bool:
        return False


class ETLConf:
    class types:
        class EIR_1(_TypeBase):
            name = "Eir Data"
            vendor = None
            radio = None
            type = "EIR"
            source = "file"
            desc = "Table consisting EIR Event Data"
            plugin = None
            file_interval = datetime.timedelta(days=1)
            output_file_interval = datetime.timedelta(days=1)
            hadoop_output_dir = "/data/landing/g_network/nss/Event/EIR/"
            hive_table_format = "eir_{schema}"
            kpi_hadoop_output_dir = "/data/landing/g_network/nss/Event/EIR/"
            kpi_hive_table = "eir_{schema}"
            kpi_hourly_hadoop_output_dir = "/data/landing/g_network/nss/Event/KPI/EIR/"
            kpi_hourly_hive_table = "kpi_eir_{schema}_hourly"
            kpi_daily_hadoop_output_dir = (
                "/data/landing/g_network/nss/Event/KPI_Daily/EIR/"
            )
            kpi_daily_hive_table = "kpi_eir_{schema}_daily"
            lookback_hours = [24 * 2]
            run_interval_minutes = [60]
            run_at = [None]
            hadoop_raw_data_retention_hours = 24 * 30 * 12
            hadoop_agg_data_retention_hours = 24 * 30 * 12
            agg_list = ["counter", "quarter_hour", "hourly", "daily"]
            schema_list = [
                "be_newimeistats",
                "be_sysperf",
                "fe_dif_sysperf",
                "fe_sif_sysperf",
                "fe_sif_licuse",
            ]
            aggregate_process = True            

        class MQA_1(_TypeBase):
            name = "Mobile Quality Agent"
            vendor = None
            radio = None
            type = "MQA"
            source = "db"
            desc = "Table consisting MQA Event Data"
            plugin = None
            file_interval = datetime.timedelta(hours=1)
            output_file_interval = datetime.timedelta(hours=1)
            hadoop_output_dir = "/data/landing/g_network/nss/Event/MQA/"
            hive_table_format = "mqa_{schema}"
            kpi_hadoop_output_dir = "/data/landing/g_network/nss/Event/KPI/MQA/"
            kpi_hive_table = "kpi_mqa_{schema}"
            kpi_daily_hadoop_output_dir = (
                "/data/landing/g_network/nss/Event/KPI_Daily/MQA/"
            )
            kpi_daily_hive_table = "kpi_mqa_{schema}_daily"
            kpi_weekly_hadoop_output_dir = (
                "/data/landing/g_network/nss/Event/KPI_Weekly/MQA/"
            )
            kpi_weekly_hive_table = "kpi_mqa_{schema}_weekly"
            kpi_monthly_hadoop_output_dir = (
                "/data/landing/g_network/nss/Event/KPI_Monthly/MQA/"
            )
            kpi_monthly_hive_table = "kpi_mqa_{schema}_monthly"
            kpi_quarterly_hadoop_output_dir = (
                "/data/landing/g_network/nss/Event/KPI_Quarterly/MQA/"
            )
            kpi_quarterly_hive_table = "kpi_mqa_{schema}_quarterly"
            lookback_hours = [24 * 2]
            run_interval_minutes = [60]
            run_at = [None]
            hadoop_raw_data_retention_hours = 24 * 30 * 13
            hadoop_agg_data_retention_hours = 24 * 30 * 13
            agg_list = ["counter", "hourly", "daily", "weekly", "monthly", "quarterly"]
            schema_list = [
                "tp_app_vol",
                "tp_coverage",
                "tp_customer",
                "tp_ping",
                "tp_th_http",
                "tp_ticket",
                "tp_time_based_bearer",
                "tp_video",
                "tp_voice",
                "tp_web",
                "tp_scoring",
                "tp_shooter",
            ]
            aggregate_process = True

        class RAN_5G_HUAWEI_1(_TypeBase):
            name = "RAN 5G Huawei"
            vendor = "huawei"
            radio = "5G"
            type = "RAN"
            source = "file"
            desc = "Table consisting Huawei 5G Radio Access Network Cell Data"
            # newer config type don't have any plugins, they're builtin, leave it here for backward compatibility
            plugin = None
            file_interval = datetime.timedelta(hours=1)
            output_file_interval = datetime.timedelta(hours=1)
            hadoop_output_dir = "/data/landing/g_network/nss/PMData/RAN5G/Huawei/"
            hive_table_format = "5G_ran_huawei_%Y%m%d"
            kpi_hadoop_output_dir = (
                "/data/landing/g_network/nss/PMData/RAN5G_KPI_Hourly/Huawei/"
            )
            kpi_hive_table = "kpi_5G_ran_huawei_hourly"
            kpi_daily_hadoop_output_dir = (
                "/data/landing/g_network/nss/PMData/RAN5G_KPI_Daily/Huawei/"
            )
            kpi_daily_hive_table = "kpi_5G_ran_huawei_daily"
            kpi_weekly_hadoop_output_dir = (
                "/data/landing/g_network/nss/PMData/RAN5G_KPI_Weekly/Huawei/"
            )
            kpi_weekly_hive_table = "kpi_5G_ran_huawei_weekly"
            kpi_monthly_hadoop_output_dir = (
                "/data/landing/g_network/nss/PMData/RAN5G_KPI_Monthly/Huawei/"
            )
            kpi_monthly_hive_table = "kpi_5G_ran_huawei_monthly"
            lookback_hours = [24 * 7]
            run_interval_minutes = [60]
            run_at = [None]
            hadoop_raw_data_retention_hours = 24 * 30 * 12
            hadoop_agg_data_retention_hours = 24 * 30 * 12
            agg_list = ["counter", "hourly", "daily", "weekly", "monthly"]
            aggregate_process = True

        class RAN_4G_HUAWEI_2(_TypeBase):
            name = "RAN 4G Huawei"
            vendor = "huawei"
            radio = "4G"
            type = "RAN"
            source = "file"
            desc = "Table consisting Huawei 4G Radio Access Network Cell Data"
            # newer config type don't have any plugins, they're builtin, leave it here for backward compatibility
            plugin = None
            file_interval = datetime.timedelta(hours=1)
            output_file_interval = datetime.timedelta(hours=1)
            hadoop_output_dir = "/data/landing/g_network/nss/PMData/RAN4G/Huawei/"
            hive_table_format = "4G_ran_huawei_%Y%m%d"
            kpi_hadoop_output_dir = (
                "/data/landing/g_network/nss/PMData/RAN4G_KPI_Hourly/Huawei/"
            )
            kpi_hive_table = "kpi_4G_ran_huawei_hourly"
            kpi_daily_hadoop_output_dir = (
                "/data/landing/g_network/nss/PMData/RAN4G_KPI_Daily/Huawei/"
            )
            kpi_daily_hive_table = "kpi_4G_ran_huawei_daily"
            kpi_weekly_hadoop_output_dir = (
                "/data/landing/g_network/nss/PMData/RAN4G_KPI_Weekly/Huawei/"
            )
            kpi_weekly_hive_table = "kpi_4G_ran_huawei_weekly"
            kpi_monthly_hadoop_output_dir = (
                "/data/landing/g_network/nss/PMData/RAN4G_KPI_Monthly/Huawei/"
            )
            kpi_monthly_hive_table = "kpi_4G_ran_huawei_monthly"
            lookback_hours = [24 * 7]
            run_interval_minutes = [60]
            run_at = [None]
            hadoop_raw_data_retention_hours = 24 * 30 * 12
            hadoop_agg_data_retention_hours = 24 * 30 * 12
            agg_list = ["counter", "hourly", "daily", "weekly", "monthly"]
            aggregate_process = True

    type_dict = OrderedDict(
            [
                ("EIR_1", types.EIR_1),
                ("MQA_1", types.MQA_1),
                ("5G_RAN_HUA_1", types.RAN_5G_HUAWEI_1),
                ("4G_RAN_HUA_2", types.RAN_4G_HUAWEI_2), 
                ]
            )

    def __init__(self, etl_type: str):
        self.etl_type_str: str = etl_type
        value = self.type_dict.get(self.etl_type_str)
        if value is not None:
            self.etl_type: _TypeBase = value()
        else:
            raise ValueError(
                "ETL Type '%s' isn't supported. Choices : %s"
                % (
                    self.etl_type_str,
                    str(list(self.type_dict.keys())),
                )
            )

    def get_info(
        self,
        filename: str,
        parent_dir_name: str = "",
        earliest_mdtm: str = "",
        latest_mdtm: str = "",
    ) -> Tuple[Optional[datetime.datetime], Optional[datetime.datetime], Optional[str]]:
        if self.etl_type_str in {
            "5G_RAN_ZTE_1",
            "4G_RAN_ZTE_1",
            "TRANSPORT_ZTE_1",
            "3G_RAN_ZTE_1",
            "2G_RAN_ZTE_1",
            "4G_RAN_ZTE_2",
            "3G_RAN_ZTE_2",
            "2G_RAN_ZTE_2",
            "4G_RAN_ZTE_USO_1",
            "4G_RAN_ZTE_USO_2",
            "2G_RAN_ZTE_USO_1",
        }:
            return self._get_info_zte(filename)
        elif self.etl_type_str in {
            "PAGING_2G_BSC_1",
            "XPU_DSP_6910_BSC_1",
            "5G_RAN_HUA_1",
            "4G_RAN_HUA_2",
            "TRANSPORT_HUA_1",
            "CORE_PS_HUA_1",
            "3G_RAN_HUA_1",
            "2G_RAN_HUA_1",
            "3G_RAN_HUA_CE_1",
            "3G_RAN_HUA_ULO_1",
            "4G_RAN_HUA_USO_1",
            "4G_RAN_HUA_USO_2",
            "2G_RAN_HUA_USO_1",
            "2G_RAN_HUA_USO_2",
            "CORE_GGSN_HUA_1",
            "CORE_SGSN_HUA_1",
            "CORE_5G_HUAWEI_1",
            "TRAFFIC_TRANSPORT_IMS_1",
            "TRAFFIC_TRANSPORT_IMS_2",
            "CH_FEGE_BSC_1",
            "SHT_IUB_IP_1",
            "IMS_TRANSPORT_5G_1",
            "XPU_6900_BSC_1",
            "DSP_6900_BSC_1",
        }:
            return self._get_info_huawei_scheduler(filename)
        elif self.etl_type_str in {
            "4G_RAN_HUA_1",
            "4G_RAN_NOK_1",
            "TRANSPORT_NOK_1",
            "4G_RAN_ERI_1",
            "CORE_PS_2G_NOK_1",
            "CORE_PS_3G_NOK_1",
            "CORE_PS_4G_NOK_1",
            "CORE_CS_NOK_1",
            "3G_RAN_ERI_1",
            "2G_RAN_ERI_1",
            "TRANSPORT_ERI_1",
            "3G_RAN_NOK_1",
            "2G_RAN_NOK_1",
            "CORE_DNS_GI_1",
            "CORE_F5_1",
            "CORE_DNS_GN_1",
            "5G_RAN_ERI_1",
            "CORE_MSS_NOK_1",
            "CORE_MGW_NOK_1",
        }:
            return self._get_info_general(filename)
        elif self.etl_type_str in {"CORE_CS_ERI_1", "CORE_PS_ERI_1"}:
            return self._get_info_core_ericsson(filename)
        elif self.etl_type_str in {"CORE_CS_ERI_2", "CORE_PS_ERI_2"}:
            return self._get_info_core_ericsson_2(filename)
        elif self.etl_type_str in {"DSP_1", "DSP_3"}:
            return self._get_info_dsp(filename)
        elif self.etl_type_str == "DSP_2":
            return self._get_info_dsp_hourly(filename)
        elif self.etl_type_str == "RDR_1":
            return self._get_info_rdr(filename)
        elif self.etl_type_str in {
            "CORE_DRA_1",
            "CORE_SCP_1",
            "CORE_HSS_CAP_1",
            "TWAMP_ERI_1",
            "OSSERA_LINK_1",
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
            "DCO_1",
            "OSSERA_PORT_1",
            "OSSERA_DEVICE_1",
            "OSSERA_DEVICE_2",
            "OSSERA_MEMORY_1",
            "OSSERA_RAN_1",
            "OSSERA_METRO_1",
            "OSSERA_GPON_1",
            "CHRONO_4G_LACIMA_USO_1",
            "CHRONO_2G_LACIMA_USO_1",
            "CHRONO_AUDITLONGLAT_1",
            "NIO_SMY_BCP_CATEGORISATION_DD_1",
            "NIO_SMY_BCP_USAGE_DD_1",
            "NIO_SMY_BCP_USAGE_HH_1",
            "NIO_V_ABT_STG_BCP_USAGE_DD_1",
            "NIO_V_ABT_SUMM_BCP_USAGE_DD_1",
            "NIO_V_SMY_BCP_CATEGORISATION_DD_1",
        }:
            return self._get_info_core_pm_daily(filename)
        elif self.etl_type_str in {
            "SMARTCARE_USERPLANE_FILEACCESS_1",
            "SMARTCARE_USERPLANE_VOIP_1",
            "SMARTCARE_USERPLANE_IM_1",
            "SMARTCARE_USERPLANE_WEB_1",
            "SMARTCARE_USERPLANE_STREAM_1",
        }:
            return self._get_info_smartcare(filename)
        elif self.etl_type_str in {
            "SMARTCARE_SDR_DYN_REGION_HTTP_SR_1",
            "SMARTCARE_SDR_DYN_NATIONAL_HTTP_SR_1",
            "SMARTCARE_SDR_DYN_REGION_ALL_IP_1",
        }:
            return self._get_info_smartcare_sdr(filename)
        elif self.etl_type_str in {
            "SHI_CM_INVENTORY_HWSW_CONTROLLER_1",
            "SHI_CM_INVENTORY_HWSW_BTS_1",
        }:
            return self._get_info_shi_inventory_hwsw(filename)
        elif self.etl_type_str in {
            "SHI_CM_INVENTORY_LICENSE_BTS_CONTROLLER_1",
            "SHI_CM_INVENTORY_LICENSE_BTS_CONTROLLER_2",
        }:
            return self._get_info_shi_inventory_license(filename)
        elif self.etl_type_str in {
            "SHI_CONFIGURATION_EAS_BTS_EMS_1",
            "SHI_CONFIGURATION_EAS_CONTROLLER_EMS_1",
        }:
            return self._get_info_shi_configuration_eas_ems(filename)
        elif self.etl_type_str in {
            "SHI_CONFIGURATION_EAS_BTS_UME_1",
            "SHI_CONFIGURATION_EAS_CONTROLLER_UME_1",
        }:
            return self._get_info_shi_configuration_eas_ume(filename)
        elif self.etl_type_str in {
            "CORE_REALTIME_CMM_NOKIA_1",
            "SHI_CM_INVENTORY_1",
            "SHI_REFERENCE_CONTROLLER_1",
            "SHI_ALARM_CODES_1",
            "SHI_ALARM_CODES_2",
            "SHI_EID_EOS_LIFE_CYCLE_SOFTWARE_1",
            "SHI_EID_EOS_LIFE_CYCLE_HARDWARE_1",
            "SHI_ZTE_EOS_LIFE_CYCLE_SOFTWARE_1",
            "SHI_ZTE_EOS_LIFE_CYCLE_1",
            "SHI_ZTE_EOS_LIFE_CYCLE_2",
            "SHI_HWI_EOS_LIFE_CYCLE_HARDWARE_1",
            "SHI_HWI_EOS_LIFE_CYCLE_SOFTWARE_1",
            "SHI_MACADDRESS_1",
            "SHI_MACADDRESS_CONTROLLER_1",
        }:
            return self._get_info_core_realtime_nokia(filename)
        elif self.etl_type_str in {
            "SHI_ZTE_CPU_LOAD_1",
            "SHI_ZTE_PAGING_LOAD_1",
            "SHI_ZTE_UTILISASI_1",
            "SHI_ZTE_DATA_INTERFACE_1",
            "SHI_ZTE_STATUS_MODUL_1",
        }:
            return self._get_info_shi_zte_hourly(filename)
        elif self.etl_type_str in {
            "SHI_ZTE_POWER_CONSUMPTION_BTS_1",
        }:
            return self._get_info_shi_zte_daily(filename)
        elif self.etl_type_str in {
            "SHI_ERI_CPU_LOAD_SITE_LEVEL_1",
            "SHI_ERI_CPU_LOAD_LEVEL_CONTROLLER_1",
            "SHI_ERI_INVENTORY_HW_CONTROLLER_1",
            "SHI_ERI_INVENTORY_HW_BTS_1",
            "SHI_ERI_EAS_SITE_BTS_1",
        }:
            return self._get_info_shi_eri_cpu_load_site_level(filename)
        elif self.etl_type_str in {
            "SHI_ERI_INVENTORY_LICENSE_FEATURE_PARAMETER_CONTROLLER_3G_RNC_1",
            "SHI_ERI_INVENTORY_LICENSE_FEATURE_PARAMETER_BTS_1",
            "SHI_ERI_UTILISASI_IUPS_IUCS_1",
        }:
            return self._get_info_shi_eri_inventory_licence(filename)
        elif self.etl_type_str in {
            "SHI_ERI_UTILISASI_IUB_S1_4G_S1_5G_1",
        }:
            return self._get_info_shi_eri_utilisasi_iub(filename)
        elif self.etl_type_str in {
            "SHI_ERI_EAS_CONTROLLER_BSC_1",
        }:
            return self._get_info_shi_eri_eas_controller(filename)
        elif self.etl_type_str in {"SHI_LICENCE_1"}:
            return self._get_info_shi_licence(filename)
        elif self.etl_type_str in {"SHI_VLAN_BTS_1"}:
            return self._get_info_shi_vlan_bts(filename)
        elif self.etl_type_str in {"SHI_NE_RAN_EXPORT_1", "SHI_VLAN_1", "SHI_VLAN_2"}:
            return self._get_info_shi_ne_ran_export(filename)
        elif self.etl_type_str in {"SHI_PARAMETER_1", "SHI_PM_EXPORT_1"}:
            return self._get_info_shi_parameter(filename)
        elif self.etl_type_str in {"CORE_REALTIME_NOKIA_CS_1"}:
            return self._get_info_core_realtime_nokia_cs1(filename)
        elif self.etl_type_str in {"CORE_NEAR_REALTIME_HUA_1"}:
            return self._get_info_core_near_realtime_huawei(filename)
        elif self.etl_type_str in {"WTTX_ORBIT_1"}:
            return self._get_info_wttx_orbit(filename)
        elif self.etl_type_str in {"WTTX_MSISDN_REFERENCE_1"}:
            return self._get_info_wttx_msisdn_reference(filename)
        elif self.etl_type_str in {
            "NIO_V_ABT_STG_BCP_USAGE_MM_1",
            "NIO_V_ABT_SUMM_BCP_USAGE_MM_1",
            "NIO_V_BCPUSG_APPSCAT_APPS_COMP_1",
            "NIO_V_STG_ABT_BCP_APP_1",
            "NIO_V_STG_ABT_BCP_CAT_1",
            "NIO_V_STG_ABT_BCP_SCORE_APP_1",
            "NIO_V_STG_ABT_BCP_SCORE_CAT_1",
        }:
            return self._get_info_niometric_monthly(filename)
        elif self.etl_type_str in {
            "CORE_OSSERA_1",
            "TWAMP_HUA_1",
            "TWAMP_NOK_1",
            "AT_NOK_CMM_1",
            "OSSERA_KPI_1",
            "OSSERA_VRF_1",
            "CHRONO_5G_LACIMA_1",
            "CHRONO_4G_LACIMA_1",
            "CHRONO_3G_LACIMA_1",
            "CHRONO_2G_LACIMA_1",
            "OSSERA_IPWAN_1",
            "OSSERA_IPRAN_1",
            "SPS_M3UA_1",
            "SPS_LICENSE_1",
            "SPS_M2PA_1",
            "IMS_CSCF_1",
            "AT_HUA_PROVISIONING_1",
        }:
            return self._get_info_core_pm_general(filename)
        elif self.etl_type_str == "CORE_MICS_RES_1":
            return self._get_info_core_mics_daily(filename)
        elif self.etl_type_str == "CORE_MICS_1":
            return self._get_info_core_mics_hourly(filename)
        elif self.etl_type_str in {
            "CORE_UPCC_1",
            "CORE_HLR_1",
            "CORE_HSS_1",
            "CORE_DSP_1",
            "CORE_SCP_UANGEL_1",
        }:
            return self._get_info_core_pm_general_15m(filename)
        elif self.etl_type_str == "TRAFFICA_1":
            return self._get_info_traffica(filename)
        elif self.etl_type_str == "TRAFFICA_PS_2":
            return self._get_info_core_pm_general_15m(filename)
        elif self.etl_type_str == "TRAFFICA_CS_2":
            return self._get_info_core_pm_general_15m(filename)
        elif self.etl_type_str in {"MQA_1", "MQA_2"}:
            return self._get_info_mqa(filename)
        elif self.etl_type_str == "VAS_SMSC_1":
            return self._get_info_vas_smsc(filename)
        elif self.etl_type_str == "VAS_MMSC_1":
            return self._get_info_vas_mmsc(filename)
        elif self.etl_type_str == "TWAMP_ZTE_1":
            return self._get_info_twamp_zte(filename)
        elif self.etl_type_str == "TWAMP_BRIX_1":
            return self._get_info_twamp_brix(filename)
        elif self.etl_type_str == "AT_HUA_1":
            return self._get_info_hua_audit_trail(filename)
        elif self.etl_type_str == "AT_HUA_PROV_1":
            return self._get_info_hua_prov_audit_trail(filename)
        elif self.etl_type_str == "AT_HUA_PROV_2":
            return self._get_info_hua_prov_2_audit_trail(filename)
        elif self.etl_type_str == "AT_ERI_LOGIN_1":
            return self._get_info_eri_login_audit_trail(filename)
        elif self.etl_type_str == "AT_NOK_LOGIN_1":
            return self._get_info_nok_login_audit_trail(filename, parent_dir_name)
        elif self.etl_type_str == "AT_HUA_PROV_3":
            return self._get_info_hua_prov_3_audit_trail(filename)
        elif self.etl_type_str == "AT_HUA_SCP_1":
            return self._get_info_hua_scp_audit_trail(filename)
        elif self.etl_type_str == "AT_NOK_MICS_1":
            return self._get_info_nokia_mics_audit_trail(filename)
        elif self.etl_type_str == "AT_ERI_MGW_1" or self.etl_type_str == "AT_ERI_MSS_1":
            return self._get_info_eri_mgw_audit_trail(filename)
        elif (
            self.etl_type_str == "AT_NOK_MSS_1"
            or self.etl_type_str == "AT_NOK_SGSN_1"
            or self.etl_type_str == "AT_NOK_MGW_1"
        ):
            return self._get_info_nokia_mss_file_audit_trail(filename, parent_dir_name)
        elif (
            self.etl_type_str == "AT_UANGEL_SCP_1"
            or self.etl_type_str == "AT_UANGEL_DRA_1"
        ):
            return self._get_info_uangel_audit_trail(filename)
        elif self.etl_type_str in {
            "AT_ERI_GGSN_1",
            "AT_ERI_GGSN_2",
            "AT_ERI_SGSN_1",
            "AT_ERI_SGSN_2",
        }:
            return self._get_info_ericsson_audit_trail_rotate(
                filename, earliest_mdtm, latest_mdtm
            )
        elif self.etl_type_str == "GEOLOC_1":
            return self._get_info_geoloc(filename)
        elif self.etl_type_str == "LACIMA_1":
            return self._get_info_lacima(filename)
        elif self.etl_type_str == "AT_NOK_OMGW_1":
            return self._get_info_nokia_omgw(
                parent_dir_name, earliest_mdtm, latest_mdtm
            )
        elif self.etl_type_str in {"4G_RAN_KPI_1", "3G_RAN_KPI_1"}:
            return self._get_info_ran_kpi_1(filename)
        elif self.etl_type_str in {
            "OSSERA_LINKREPORT_1",
            "OSSERA_CARDS_1",
            "SPEEDTEST_CSENTIMEN_1",
        }:
            return self._get_info_ossera_linkreport(filename)
        elif self.etl_type_str in {
            "OSSERA_LV_BH_4G_1",
            "OSSERA_QC_4G_1",
        }:
            return self._get_info_ossera_qc_lv(filename)
        elif self.etl_type_str in {
            "OSSERA_RAW_3G_1",
            "OSSERA_RAW_4G_1",
        }:
            return self._get_info_ossera_raw(filename)
        elif self.etl_type_str in {
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
            "CHRONO_BIGDATA_HUA_UCELL_1",
            "CHRONO_BIGDATA_HUA_NODEB_1",
            "CHRONO_BIGDATA_ZTE_ENODEB_1",
            "CHRONO_BIGDATA_NOK_ENODEB_1",
            "CHRONO_BIGDATA_ERI_ENODEB_1",
            "CHRONO_BIGDATA_ERI_NODEB_1",
            "CHRONO_BIGDATA_ERI_UCELL_1",
            "CHRONO_BIGDATA_NOK_UCELL_1",
            "CHRONO_BIGDATA_ZTE_UCELL_1",
            "CHRONO_BIGDATA_HUA_ENODEB_1",
            "CHRONO_BIGDATA_ERI_GCELL_1",
            "CHRONO_BIGDATA_HUA_GCELL_1",
            "CHRONO_BIGDATA_NOK_GCELL_1",
            "CHRONO_BIGDATA_ZTE_GCELL_1",
            "CHRONO_BIGDATA_HUA_BTS_1",
            "CHRONO_BIGDATA_NOK_GTRX_1",
        }:
            return self._get_info_chrono(filename)
        elif self.etl_type_str in {
            "CHRONO_LACIMA_LONGLAT_1",
            "CHRONO_4G_NSTO_1",
            "CHRONO_2G_NSTO_USO_1",
            "CHRONO_NSTO_LIST_ENODEB_1",
            "CHRONO_2G_NSTO_1",
            "CHRONO_4G_NSTO_USO_1",
            "CHRONO_3G_NSTO_1",
            "CHRONO_5G_LACIMA_ENR_1",
            "CHRONO_4G_LACIMA_ENR_1",
            "CHRONO_3G_LACIMA_ENR_1",
            "CHRONO_2G_LACIMA_ENR_1",
            "CHRONO_2G_LACIMA_USO_ENR_1",
            "CHRONO_4G_LACIMA_USO_ENR_1",
        }:
            return self._get_info_lacima_longlat_rotate(
                filename, earliest_mdtm, latest_mdtm
            )
        elif self.etl_type_str in {"ISD_1"}:
            return self._get_info_isd(filename)
        elif self.etl_type_str in {"TA_ERI_1"}:
            return self._get_info_core_pm_general(filename)
        elif self.etl_type_str in {"EIR_1"}:
            return self._get_info_eir(filename)
        elif self.etl_type_str in {"SBC_2"}:
            return self._get_info_sbc_cdr(filename)
        elif self.etl_type_str in {"SBC_1"}:
            return self._get_info_sbc(filename)
        elif self.etl_type_str in {"B2B_APN_CORPORATE_1"}:
            return self._get_info_b2b_apn(filename)
        elif self.etl_type_str in {"B2B_APN_ALARM_1"}:
            return self._get_info_b2b_apn_alarm(filename)
        elif self.etl_type_str in {"OSSERA_QOS_1"}:
            return self._get_info_ossera_qos(filename)
        elif self.etl_type_str in {"SPEEDTEST_SPEED_1", "SPEEDTEST_COVERAGE_1"}:
            return self._get_info_speedtest(filename)
        elif self.etl_type_str in {"GRAFANA_1"}:
            return self._get_info_grafana(filename)
        elif self.etl_type_str in {"NVS_SBC_P_CSCF_1"}:
            return self._get_info_nvs(filename)
        elif self.etl_type_str in {"NVS_S_CSCF_1"}:
            return self._get_info_nvs_scscf(filename)
        elif self.etl_type_str in {"NVS_NTAS_1"}:
            return self._get_info_nvs_ntas(filename)
        elif self.etl_type_str in {"REDCELL_1"}:
            return self._get_info_redcell_daily(filename)
        elif self.etl_type_str in {
            "REDCELL_TA_HUA_1",
            "REDCELL_TA_NOK_1",
            "REDCELL_TA_ERI_1",
            "REDCELL_TA_ZTE_1",
        }:
            return self._get_info_redcell_ta(filename)
        elif self.etl_type_str in {"NVS_TITAN_ENUM_1"}:
            return self._get_info_titan_enum(filename)
        elif self.etl_type_str in {
            "MICS_DB_COMPANY_1",
            "MICS_DB_USER_1",
            "MICS_DB_USERNUMBER_1",
        }:
            return self._get_info_mics_db(filename)
        elif self.etl_type_str in {"CORE_REALTIME_FNS_NOKIA_1"}:
            return self._get_info_core_realtime_nfs_nokia(filename)
        elif self.etl_type_str in {"CORE_REALTIME_NOKIA_CS_2"}:
            return self._get_info_core_realtime_cs_nokia_2(filename)
        elif self.etl_type_str in {"CORE_REALTIME_GGSN_ERI_1"}:
            return self._get_info_core_realtime_ggsn_eri(filename, parent_dir_name)
        elif self.etl_type_str in {"RAN_KPI_ENR_1"}:
            return self._get_info_enrichment(filename)
        elif self.etl_type_str in {"PRB_AVAIL_HUA_1"}:
            return self._get_info_prb_hua(filename)
        elif self.etl_type_str in {"DATACOMM_BACKHAUL_1", "DATACOMM_AVAILABILITY_1"}:
            return self._get_info_datacomm(filename)
        elif self.etl_type_str in {"CORE_REALTIME_CS_ERI_1"}:
            return self._get_info_core_realtime_cs_eri(filename)
        elif self.etl_type_str in {"CORE_REALTIME_PS_ERI_1"}:
            return self._get_info_core_realtime_ps_eri(filename)
        elif self.etl_type_str in {
            "OSSERA_ATOM_1",
            "OSSERA_VGGSN_1",
            "OSSERA_CNOP_1",
            "OSSERA_RAW_GINET_1",
        }:
            return self._get_info_cnop(filename)
        elif self.etl_type_str in {"OSSERA_PSGI_1", "OSSERA_IX_1"}:
            return self._get_info_cnop_psgi(filename)
        elif self.etl_type_str in {"NAURA_ZTE_1"}:
            return self._get_info_naura_zte(filename)
        elif self.etl_type_str in {"4G_VQI_ZTE_1", "3G_VQI_ZTE_1", "2G_VQI_ZTE_1"}:
            return self._get_info_vqi_zte(filename)
        elif self.etl_type_str in {"OSSERA_IX_2"}:
            return self._get_info_cnop_ix_monthly(filename)
        elif self.etl_type_str in {"FDI_ERICSSON_1", "FDI_HUAWEI_1", "FDI_ZTE_1"}:
            return self._get_info_fdi(filename)
        elif self.etl_type_str in {"FDI_USER_ALARM_CORE_1"}:
            return self._get_info_fdi_core(filename)
        elif self.etl_type_str in {"TA_ERI_2"}:
            return self._get_info_ta_eri_new(filename)
        elif self.etl_type_str in {
            "CORE_GGSN_ERI_NDW_1",
            "CORE_SGSN_ERI_NDW_1",
            "CORE_MSS_ERI_NDW_1",
            "CORE_MGW_ERI_NDW_1",
        }:
            return self._get_info_core_ggsn_eri_ndw(filename)
        elif self.etl_type_str in {
            "AT_RADIO_HUA_SECURITY_1",
            "AT_RADIO_HUA_SYSTEM_1",
            "AT_RADIO_HUA_OPERATION_1",
        }:
            return self._get_info_at_radio_hua(filename)
        elif self.etl_type_str in {
            "AT_RADIO_HUA_SECURITY_2",
            "AT_RADIO_HUA_SYSTEM_2",
            "AT_RADIO_HUA_OPERATION_2",
        }:
            return self._get_info_at_radio_hua_db(filename)
        elif self.etl_type_str in {
            "CORE_5G_ERI_GGSN_1",
        }:
            return self._get_info_core_5g_eri_ggsn(filename)
        elif self.etl_type_str in {
            "CORE_5G_ERI_SGSN_1",
        }:
            return self._get_info_core_5g_sgsn_eri(filename)
        else:
            raise ValueError("Unknown vendor")

    def _get_info_huawei_scheduler(
        self, filename: str
    ) -> Tuple[Optional[datetime.datetime], Optional[datetime.datetime], Optional[str]]:
        try:
            return self._get_info_general(filename)
        except (ValueError, IndexError):
            filename_split = filename.split("_")
            for i, filename_part in enumerate(filename_split):
                try:
                    datetime_start_tz = datetime.datetime.strptime(
                        filename_split[i].split(".")[0], "%Y%m%d%H%M"
                    ).astimezone()
                    datetime_finish_tz = datetime.datetime.strptime(
                        filename_split[i + 1].split(".")[0], "%Y%m%d%H%M"
                    ).astimezone()
                except ValueError:
                    continue
                except IndexError:
                    return None, None, None
                else:
                    if self.etl_type_str == "CORE_PS_HUA_1":
                        return datetime_start_tz, datetime_finish_tz, filename
                    elif self.etl_type_str == "CORE_GGSN_HUA_1":
                        return datetime_start_tz, datetime_finish_tz, filename
                    elif self.etl_type_str == "CORE_SGSN_HUA_1":
                        return datetime_start_tz, datetime_finish_tz, filename
                    else:
                        extra_info = "_".join(filename_split[i + 2 :])
                        return datetime_start_tz, datetime_finish_tz, extra_info
        return None, None, None

    @staticmethod
    def _get_info_general(
        filename: str,
    ) -> Tuple[Optional[datetime.datetime], Optional[datetime.datetime], Optional[str]]:
        try:
            filename_split = filename.split(
                "."
            )  # A20180514.0000+0700-0015+0700_SubNetwork=ONRM_ROOT_MO,SubNetwork=...
            date_str = filename_split[0].strip("A")  # 20180514

            interval_str = filename_split[1].split("_")[0]  # 0000+0700-0015+0700
            interval_str_split = interval_str.split("-")
            datetime_start_tz = isoparse(
                "{date}.{time}".format(date=date_str, time=interval_str_split[0])
            ).astimezone()
            datetime_finish_tz = isoparse(
                "{date}.{time}".format(date=date_str, time=interval_str_split[1])
            ).astimezone()

            if datetime_finish_tz.hour == 0 and datetime_start_tz.hour == 23:
                datetime_finish_tz += datetime.timedelta(days=1)

            extra_info = "_".join(filename_split[1].split("_")[1:])
        except (ValueError, IndexError):
            raise ValueError("Error extracting data from %s" % filename)
        else:
            return datetime_start_tz, datetime_finish_tz, extra_info
