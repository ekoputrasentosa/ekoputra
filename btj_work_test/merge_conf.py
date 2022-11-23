from abc import abstractmethod
from collections import OrderedDict
from formula import EIRFormula,MQAFormula

class _Base:
    @property
    @abstractmethod
    def hourly(self) -> OrderedDict:
        raise AttributeError

    @property
    @abstractmethod
    def hourly_sub(self) -> OrderedDict:
        raise AttributeError

    @property
    @abstractmethod
    def daily(self) -> OrderedDict:
        raise AttributeError

    @property
    @abstractmethod
    def weekly(self) -> OrderedDict:
        raise AttributeError

    @property
    @abstractmethod
    def monthly(self) -> OrderedDict:
        raise AttributeError

    @property
    @abstractmethod
    def monthly_week(self) -> OrderedDict:
        raise AttributeError

    @property
    @abstractmethod
    def is_daily(self) -> bool:
        raise AttributeError

    @property
    @abstractmethod
    def merge_hadoop(self) -> OrderedDict:
        raise AttributeError

class MergeConf:
    class types:
        class EIR_BE_NEWIMEISTATS(_Base):
            obj = EIRFormula
            merge_hadoop = obj.merge_be_newimeistats

        class EIR_BE_SYSPERF(_Base):
            obj = EIRFormula
            merge_hadoop = obj.merge_be_sysperf
            
        class EIR_FE_DIF_SYSPERF(_Base):
            obj = EIRFormula
            merge_hadoop = obj.merge_fe_dif_sysperf
            
        class EIR_FE_SIF_SYSPERF(_Base):
            obj = EIRFormula
            merge_hadoop = obj.merge_fe_sif_sysperf
            
        class EIR_FE_SIF_LICUSE(_Base):
            obj = EIRFormula
            merge_hadoop = obj.merge_fe_sif_licuse
            
        class MQA_TP_APP_VOL(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_app_vol
            
        class MQA_TP_COVERAGE(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_coverage
            
        class MQA_TP_CUSTOMER(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_customer
            
        class MQA_TP_PING(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_ping
            
        class MQA_TP_TH_HTTP(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_th_http
            
        class MQA_TP_TICKET(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_ticket
            
        class MQA_TP_TIME_BASED_BEARER(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_time_based_bearer
            
        class MQA_TP_VIDEO(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_video
            
        class MQA_TP_VOICE(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_voice
            
        class MQA_TP_WEB(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_web
            
        class MQA_TP_SCORING(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_scoring
            
        class MQA_TP_SHOOTER(_Base):
            obj = MQAFormula 
            merge_hadoop = obj.merge_tp_shooter
            
    type_dict = OrderedDict(
        [
            ("EIR_BE_NEWIMEISTATS", types.EIR_BE_NEWIMEISTATS),
            ("EIR_BE_SYSPERF", types.EIR_BE_SYSPERF),
            ("EIR_FE_DIF_SYSPERF", types.EIR_FE_DIF_SYSPERF),
            ("EIR_FE_SIF_SYSPERF", types.EIR_FE_SIF_SYSPERF),
            ("EIR_FE_SIF_LICUSE", types.EIR_FE_SIF_LICUSE),
            ("MQA_TP_APP_VOL", types.MQA_TP_APP_VOL),
            ("MQA_TP_COVERAGE", types.MQA_TP_COVERAGE),
            ("MQA_TP_CUSTOMER", types.MQA_TP_CUSTOMER),
            ("MQA_TP_PING", types.MQA_TP_PING),
            ("MQA_TP_TH_HTTP", types.MQA_TP_TH_HTTP),
            ("MQA_TP_TICKET", types.MQA_TP_TICKET),
            ("MQA_TP_TIME_BASED_BEARER", types.MQA_TP_TIME_BASED_BEARER),
            ("MQA_TP_VIDEO", types.MQA_TP_VIDEO),
            ("MQA_TP_VOICE", types.MQA_TP_VOICE),
            ("MQA_TP_WEB", types.MQA_TP_WEB),
            ("MQA_TP_SCORING", types.MQA_TP_SCORING),
            ("MQA_TP_SHOOTER", types.MQA_TP_SHOOTER),
        ]
    )

    def __init__(self, etl_type_str):
        self.etl_type_str = etl_type_str
        value = self.type_dict.get(self.etl_type_str)
        if value is not None:
            self.func: _Base = value()
        else:
            raise ValueError(
                "ETL Type '%s' isn't supported. Choices : %s"
                % (
                    self.etl_type_str,
                    str(list(self.type_dict.keys())),
                )
            )
