import datetime
import os
from merge_conf import MergeConf
from common import MergeNameId
from conf import ETLConf

def hadoop_data_merge_event_schema_partition_date_v2(
        target_date: datetime.date, etl_type_str: str
        ):
    table_data = MergeNameId(date=target_date,config_type=etl_type_str).getName()
    etl_conf = ETLConf(etl_type_str)
    schema_list = etl_conf.etl_type.schema_list

    for schema_str in schema_list:
        merge_conf_str = etl_conf.etl_type.hive_table_format.format(schema=schema_str).upper()
        agg_func = MergeConf(merge_conf_str)
        merge_table_name = target_date.strftime(
                etl_conf.etl_type.hive_table_format.format(schema=schema_str)
                )
        query = f"""
        SELECT
            {'%s.%s AS %s' % (merge_table_name, table_data['time'], table_data['time'])},
            {', '.join(['%s.%s AS %s' % (merge_table_name,d,d) for d in table_data['id']])},
            {', '.join(['%s AS %s' % (k, k) for k, _ in agg_func.func.merge_hadoop.items()])}
        FROM {merge_table_name}
        WHERE `date` = '{target_date.strftime('%Y%m%d')}'
        """

        hadoop_dest_dir = etl_conf.etl_type.hadoop_output_dir + schema_str.upper()
        partition_path = os.path.join(hadoop_dest_dir, target_date.strftime("%Y%m%d/"))

        print("partition path",partition_path)
        
        data_type = []
        data_type.append(
                {
                    "name": table_data["time"],
                    "type": "BIGINT",
                    }
                )
        
        for table_id in table_data["id"]:
            type_exist = table_data.get("id_type_bigint",None)
            id_type = "STRING"
            if type_exist:
                if table_id in table_data["id_type_bigint"]:
                    id_type = "BIGINT"
            data_type.append(
                    {
                        "name": table_id,
                        "type": id_type,
                        }
                    )

        for value_id,id_type in agg_func.func.merge_hadoop.items():
            data_type.append(
                    {
                        "name": value_id,
                        "type": id_type.upper(),
                        }
                    )

        for idx, value in enumerate(data_type):
            if "`" in value["name"]:
                data_type[idx]["name"] = value["name"].replace("`","")
                break
            # if value["name"] == "`percent`":
            #     data_type[idx]["name"] = "percent"
            #     break
            # elif value["name"] == "`timestamp`":
            #     data_type[idx]["name"] = "timestamp"
            #     break

        schema_type = ", ".join([f'`{d["name"]}` {d["type"]}' for d in data_type])
        for schema in schema_type.split(', '):
            print(schema)
        hive_table = etl_conf.etl_type.hive_table_format.format(schema=schema_str)
        print("query...")
        print(query)
        print("="*50,"\n")
        print()
        print()
    

today = datetime.date.today()
target_date = today - datetime.timedelta(days=120)

default_config_mqa = {
        "MQA_1":
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
            "id_double": [
                "hour",
            ],
        },
    }



for etl_type_str in ETLConf.type_dict:
    print("ETL TYPE STR--->",etl_type_str)
    print("@"*160)
    print()
    print()

    hadoop_data_merge_event_schema_partition_date_v2(target_date,etl_type_str)
    # table_data = default_config_mqa["MQA_1"]
    # for idx,value in enumerate(table_data["id"]):
    #     if value in table_data["id_double"]:
    #         print(value)
