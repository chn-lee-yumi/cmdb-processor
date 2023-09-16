import datetime
import json
import pymysql  # pymysql
from kafka import KafkaConsumer  # kafka-python
from pymysql.converters import escape_string

#db = pymysql.connect(host="10.0.0.5", user="root", passwd="123456", db="cmdb")
db = pymysql.connect(host="10.9.0.5", user="cmdb", passwd="cmdb", db="cmdb")
cur = db.cursor()


def write_db(msg):
    # 先判断主ip是否在数据库中
    cur.execute("SELECT id FROM machine WHERE main_ip='%s'" % (msg["network_info"]["main_ip"]))
    machine = cur.fetchone()
    if machine:  # 如果在就执行UPDATE
        cur.execute(
            "UPDATE machine SET main_ip='%s',device_system_info='%s',system_info='%s',cpu_info='%s',memory_info='%s',load_avg='%s',interfaces='%s',last_heartbeat='%s' WHERE id='%s'" %
            (
                msg["network_info"]["main_ip"],
                escape_string(json.dumps(msg["device_system_info"])),
                escape_string(json.dumps(msg["system_info"])),
                escape_string(json.dumps(msg["cpu_info"])),
                escape_string(json.dumps(msg["memory_info"])),
                escape_string(json.dumps(msg["load_avg"])),
                escape_string(json.dumps(msg["network_info"]["interfaces"])),
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                machine[0],
            ))
        cur.execute("COMMIT")
    # 如果不在就忽略
    # else:  # 如果不在就执行INSERT
    #     cur.execute(
    #         "INSERT INTO machine (id,main_ip,device_system_info,system_info,cpu_info,memory_info,load_avg,interfaces) "
    #         "VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" %
    #         (
    #             str(uuid.uuid1(random.randint(0, 2 ** 48 - 1))),
    #             msg["network_info"]["main_ip"],
    #             escape_string(json.dumps(msg["device_system_info"])),
    #             escape_string(json.dumps(msg["system_info"])),
    #             escape_string(json.dumps(msg["cpu_info"])),
    #             escape_string(json.dumps(msg["memory_info"])),
    #             escape_string(json.dumps(msg["load_avg"])),
    #             escape_string(json.dumps(msg["network_info"]["interfaces"])),
    #         ))
    # cur.execute("COMMIT")


def run_consumer():
    consumer = KafkaConsumer(
        'cmdb-receiver',
        #bootstrap_servers='10.0.0.5:9092',
        bootstrap_servers='kafka.nic-kafka.svc.cluster.local:9092',
        value_deserializer=json.loads,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="processor"
    )
    for message in consumer:
        # 将数据写入mysql
        print(message.partition, message.offset, message.timestamp)
        write_db(message.value)
        consumer.commit()


if __name__ == '__main__':
    run_consumer()
