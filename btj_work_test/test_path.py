import os
from conf import ETLConf
import datetime

HOST_TARGET_PARQUET = {
    "host": "10.54.18.250",
    "port": 22,
    "method": "sftp",
    "username": "nbpa",
    "password": "nbpa@123",
    "path": "/mnt/storage/ultimate/nbpa/radio/"
}

now = datetime.datetime.now()

host_target = HOST_TARGET_PARQUET

etl_conf = ETLConf("5G_RAN_HUA_1")

path_destination_remote = os.path.join(
    host_target["path"],
    etl_conf.etl_type.vendor,
    etl_conf.etl_type.radio,
    etl_conf.etl_type.kpi_hadoop_output_dir.split("/")[-3],
    now.strftime("%Y%m%d"),
)

print(path_destination_remote)