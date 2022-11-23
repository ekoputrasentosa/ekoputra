from huawei import Huawei
from conf import ETLConf
import re


def regex_pattern():
    compiled_regex_pattern = {
            "4G_RAN_HUA_2": re.compile(
                "|".join(Huawei.scheduler.radio_4g), re.IGNORECASE
            ),
        }
    return compiled_regex_pattern

def check_if_file_should_get(info_tuple,etl_conf,ip=None):
    datetime_start_tz, datetime_finish_tz, extra_info = info_tuple
    if datetime_start_tz is None or datetime_finish_tz is None:
        print("Date empty")
    pattern = regex_pattern()
    match = pattern[etl_conf.etl_type_str].match(extra_info)
    if match and etl_conf.etl_type.file_interval == (datetime_finish_tz - datetime_start_tz):
        return True
    return False

etl_conf = ETLConf("4G_RAN_HUA_2")

with open("result_ls.txt","r") as file:
    count_file = 0
    filename = file.readlines()
    print("Count file before check",len(filename))
    print("#"*70)
    print()
    for name in filename[:600]:
    # for name in filename:
        name = name[:-1]
        info_tuple = etl_conf.get_info(name)
        should_get = check_if_file_should_get(info_tuple,etl_conf)
        if should_get:
            datetime_start_tz, datetime_finish_tz, extra_info = info_tuple
            print("Info date")
            print(name)
            print(datetime_start_tz)
            print(datetime_finish_tz)
            print(extra_info)
            count_file += 1
    
    print()
    print("#"*70)
    print("Count file after check",count_file)
