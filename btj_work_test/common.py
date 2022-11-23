import datetime
from typing import Dict, List, Union

class MergeNameId:
    _nameId_by_date_dict_eir = {
            "EIR_1": [
            {
                "config_date": "2019-03-20",
                "result": {
                    "time": "datetime",
                    "id": [
                        "ip_source",
                        "hour",
                        "date_source",
                    ],
                    "name": [
                        "ip_source",
                        "hour",
                        "date_source",
                    ],
                }
            }
        ],
    }

    _default_config_eir = {
            "EIR_1":
            {
                "time": "datetime",
                "id": [
                    "ip_source",
                    "hour",
                    "date_source",
                ],
                "name": [
                    "ip_source",
                    "hour",
                    "date_source",
                ],
            },
        }

    _nameId_by_date_dict_mqa = {
            "MQA_1": [
            {
                "config_date": "2019-03-20",
                "result": {
                    "time": "`timestamp`",
                    "id": [
                        "filename",
                        "number_of_record",
                        "cell_id",
                        "counter_not_null",
                        "area",
                        "branch",
                        "cluster",
                        "desa",
                        "kabupaten",
                        "kecamatan",
                        "provinsi",
                        "region",
                        "region_oms",
                        "subbranch",
                        "id_area",
                        "id_branch",
                        "id_cluster",
                        "id_desa",
                        "id_kab",
                        "id_kec",
                        "id_prov",
                        "id_reg",
                        "id_reg_oms",
                        "id_subbranch",
                        "yearmonth",
                        "yearquarterly",
                        "yearweek",
                        "timezone_group",
                    ],
                    "name": [
                        "filename",
                        "number_of_record",
                        "cell_id",
                        "counter_not_null",
                        "area",
                        "branch",
                        "cluster",
                        "desa",
                        "kabupaten",
                        "kecamatan",
                        "provinsi",
                        "region",
                        "region_oms",
                        "subbranch",
                        "id_area",
                        "id_branch",
                        "id_cluster",
                        "id_desa",
                        "id_kab",
                        "id_kec",
                        "id_prov",
                        "id_reg",
                        "id_reg_oms",
                        "id_subbranch",
                        "yearmonth",
                        "yearquarterly",
                        "yearweek",
                        "timezone_group",
                    ],
                    "id_type_bigint": {
                        "number_of_record",
                        "counter_not_null",
                    },
                }
            }
        ],
    }

    _default_config_mqa = {
            "MQA_1":
            {
                "time": "`timestamp`",
                "id": [
                    "filename",
                    "number_of_record",
                    "cell_id",
                    "counter_not_null",
                    "area",
                    "branch",
                    "cluster",
                    "desa",
                    "kabupaten",
                    "kecamatan",
                    "provinsi",
                    "region",
                    "region_oms",
                    "subbranch",
                    "id_area",
                    "id_branch",
                    "id_cluster",
                    "id_desa",
                    "id_kab",
                    "id_kec",
                    "id_prov",
                    "id_reg",
                    "id_reg_oms",
                    "id_subbranch",
                    "yearmonth",
                    "yearquarterly",
                    "yearweek",
                    "timezone_group",
                ],
                "name": [
                    "filename",
                    "number_of_record",
                    "cell_id",
                    "counter_not_null",
                    "area",
                    "branch",
                    "cluster",
                    "desa",
                    "kabupaten",
                    "kecamatan",
                    "provinsi",
                    "region",
                    "region_oms",
                    "subbranch",
                    "id_area",
                    "id_branch",
                    "id_cluster",
                    "id_desa",
                    "id_kab",
                    "id_kec",
                    "id_prov",
                    "id_reg",
                    "id_reg_oms",
                    "id_subbranch",
                    "yearmonth",
                    "yearquarterly",
                    "yearweek",
                    "timezone_group",
                ],
                "id_type_bigint": {
                    "number_of_record",
                    "counter_not_null",
                },
            },
        }

    def __init__(self, date: datetime.date, config_type: str):
        self.date = date
        self.config_type = config_type
        self.result = None
        self._setNameID()

    def _setNameID(self):
        if self.config_type.startswith("EIR"):
            nameIdObj = self._nameId_by_date_dict_eir
            default = self._default_config_eir
        elif self.config_type.startswith("MQA"):
            nameIdObj = self._nameId_by_date_dict_mqa
            default = self._default_config_mqa
        else:
            raise ValueError(f"Vendor {self.config_type} doesn't exist.")
        config = nameIdObj.get(self.config_type)
        default_result = default.get(self.config_type)
        for data in config:
            temp_date = datetime.datetime.strptime(data["config_date"], "%Y-%m-%d")
            if self.date < temp_date.date():
                break
            else:
                self.result = data["result"]
        if self.result is None:
            self.result = default_result

    def getName(self) -> Dict[str, Union[str, List[str]]]:
        return self.result
