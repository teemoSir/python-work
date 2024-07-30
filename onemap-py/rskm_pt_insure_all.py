import fiona
from shapely.geometry import shape
import shapely.wkt
from shapely.wkt import dumps
import psycopg2
import requests
import time
import json
from datetime import datetime, timedelta
import hashlib
import random
import logger


# PostGIS数据库连接参数
db_host = "localhost"
db_name = "postgres"
db_user = "postgres"
db_pass = "root"
db_port = 5432

# 定义要请求的URL
url = "http://124.128.248.214:85/api/transfer"
apiKey = "nxptyzt"
privateKey = "46AA1572-L767-5783-F2A8-2A28F7240702"


# 批量
pgsql = []
pgsql_bj = []
pgsql_delete = []
list_bill = []
size = 30


# http://59.110.61.7:8092/helper/index.html
def getinsdata():
    timet = str(datetime.now() - timedelta(seconds=4))
    action = "/api/syncinsurance/getinsdata/{}".format(size)

    # print(json.dumps(headers))
    # POST请求
    headers_list = [
        {
            "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 8.0.0; SM-G955U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (iPad; CPU OS 13_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/87.0.4280.77 Mobile/15E148 Safari/604.1"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 8.0; Pixel 2 Build/OPD3.170816.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.109 Safari/537.36 CrKey/1.54.248666"
        },
        {
            "user-agent": "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.188 Safari/537.36 CrKey/1.54.250320"
        },
        {
            "user-agent": "Mozilla/5.0 (BB10; Touch) AppleWebKit/537.10+ (KHTML, like Gecko) Version/10.0.9.2372 Mobile Safari/537.10+"
        },
        {
            "user-agent": "Mozilla/5.0 (PlayBook; U; RIM Tablet OS 2.1.0; en-US) AppleWebKit/536.2+ (KHTML like Gecko) Version/7.2.1.0 Safari/536.2+"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; U; Android 4.3; en-us; SM-N900T Build/JSS15J) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; U; Android 4.1; en-us; GT-N7100 Build/JRO03C) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; U; Android 4.0; en-us; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 7.0; SM-G950U Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.84 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 8.0.0; SM-G965U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.111 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 8.1.0; SM-T837A) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.80 Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; U; en-us; KFAPWI Build/JDQ39) AppleWebKit/535.19 (KHTML, like Gecko) Silk/3.13 Safari/535.19 Silk-Accelerated=true"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; U; Android 4.4.2; en-us; LGMS323 Build/KOT49I.MS32310c) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 550) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Mobile Safari/537.36 Edge/14.14263"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0.1; Moto G (4)) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 10 Build/MOB31T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 8.0.0; Nexus 5X Build/OPR4.170623.006) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 7.1.1; Nexus 6 Build/N6F26U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 8.0.0; Nexus 6P Build/OPP3.170518.006) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 7 Build/MOB30X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 520)"
        },
        {
            "user-agent": "Mozilla/5.0 (MeeGo; NokiaN9) AppleWebKit/534.13 (KHTML, like Gecko) NokiaBrowser/8.5.0 Mobile Safari/534.13"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 9; Pixel 3 Build/PQ1A.181105.017.A1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 10; Pixel 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 11; Pixel 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.181 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 8.0; Pixel 2 Build/OPD3.170816.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
        },
        {
            "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1"
        },
        {
            "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1"
        },
        {
            "user-agent": "Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1"
        },
    ]

    # 定义请求头
    headers = {
        "Content-Type": "application/json",
        "ApiKey": apiKey,
        "Sign": get_md5_sum(
            "apikey={}&time={}&key={}".format(apiKey, timet, privateKey)
        ),
        "Time": timet,
        "Action": action,
        "user-agent": random.choice(headers_list)["user-agent"],
    }

    data = {"current": 1}

    response = requests.post(url, headers=headers, data=json.dumps(data), timeout=30)

    if response.status_code:
        if response.text:
            # 验证是否是完整数据
            if response.text[-2:] != "}]":
                print("异常跳过：response.text 不是完整数据")
            insurance_json = json.loads(response.text)

            for item in insurance_json:
                # print(item["geojsonstr"])
                print("==============")
                geojsonstr = item["geojsonstr"]
                # print(geojsonstr)
                print("==============")
                if len(geojsonstr) > 0:
                    # 是否需要新增
                    if str(item["updateornot"]) == "1":
                        sql = loadGeometry(item, json.loads(item["geojsonstr"]))
                        bool = True
                        for geog in sql:
                            if str(geog["wkt_geom"]) == "":
                                bool = False

                        if bool == True:
                            global pgsql
                            pgsql = pgsql + sql
                            pgsql_bj.append(1)
                        else:
                            pgsql_bj.append(-1)

                    else:
                        pgsql_delete.append(item["insurancenum"])

                else:
                    print("异常跳过：geojsonstr字段缺失")
                    pgsql_bj.append(-1)

                list_bill.append(
                    {
                        "CreatedDate": item["CreatedDate"],
                        "InsuranceId": item["InsuranceId"],
                        "LogStatus": item["LogStatus"],
                    }
                )

            if len(pgsql_bj) != len(list_bill):
                print("异常停止:返回标记pgsql_bj与list_bill提交标记长度不一致")
                return

    else:
        print("ajax请求失败" + response.status_code + response.text)


# 读取json到几何图形列
def loadGeometry(data, json):
    # print("loadGeometry")
    # print(insurance_json)
    # print(esri_json)
    listgeojson = []
    srid = ""
    if "wkid" in json["spatialReference"]:
        srid = json["spatialReference"]["wkid"]

    # 读取 Esri JSON 文件
    for feature in json["features"]:

        # 建立标准json
        geojson = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": feature["geometry"]["rings"],
            },
            "properties": feature["attributes"],
        }
        geom = None
        wkt_geom = None
        # 获取几何对象
        try:
            geom = shape(geojson["geometry"])
            # 转换为WKT格式
            wkt_geom = dumps(geom)
        except Exception as e:
            wkt_geom = ""
        finally:
            pass

        T = ""
        R = ""
        A12 = ""
        A11 = ""
        if "T" in feature["attributes"]:
            T = feature["attributes"]["T"]
        if "R" in feature["attributes"]:
            R = feature["attributes"]["R"]
        if "A12" in feature["attributes"]:
            A12 = feature["attributes"]["A12"]
        if "A11" in feature["attributes"]:
            A11 = feature["attributes"]["A11"]

        print("-{}-{}-{}-{}-".format(T, R, A12, A11))
        listgeojson.append(
            {
                "wkt_geom": wkt_geom,
                "insurancenum": data["insurancenum"],
                "insurcompanycode": data["insurcompanycode"],
                "insured": data["insured"],
                "EffectStartDate": data["EffectStartDate"],
                "EffectEndDate": data["EffectEndDate"],
                "RegionCode": data["RegionCode"],
                "InsuranceId": data["InsuranceId"],
                "CreatedDate": data["CreatedDate"],
                "insurancetarget": data["insurancetarget"],
                "insuredquantity": data["insuredquantity"],
                "srid": srid,
                "R": R,
                "T": T,
                "A12": A12,
                "A11": A11,
                "update_data":datetime.now()
            }
        )
    return listgeojson


def connPgInstall():

    # 建立数据库连接
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_pass, host=db_host, port=db_port
    )

    try:
        # 创建一个cursor对象用于执行SQL语句
        cur = conn.cursor()
        sql = "INSERT INTO public.rskm_pt(geom, insurancenum, insurcompany_code, insured, start_date, end_date, region_code, insurance_id, create_date, update_data, insurancetarget, insured_quantity,srid_data,r_data,t_data,a12_data,a11_data)VALUES (ST_SetSRID(ST_GeomFromText(%s), 4326), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        for item in pgsql:
            print("入库内容")
            print(item)
            print("")
            cur.execute(
                sql,
                (

                    item["wkt_geom"],
                    item["insurancenum"],
                    item["insurcompanycode"],
                    item["insured"],
                    item["EffectStartDate"],
                    item["EffectEndDate"],
                    item["RegionCode"],
                    item["InsuranceId"],
                    item["CreatedDate"],
                    item["update_data"],
                    item["insurancetarget"],
                    item["insuredquantity"],
                    item["srid"],
                    item["R"],
                    item["T"],
                    item["A12"],
                    item["A11"],
                ),
            )
        # print(sql)
        print("    +1批次 更新    ")

        # 提交事务
        conn.commit()

    except Exception as e:
        # global sendServer
        print(f"添加异常 跳过: {e}")
        # 如果发生错误，则回滚事务
        conn.rollback()

    finally:
        # 关闭cursor和连接
        cur.close()
        conn.close()

        # del srid


def queryDiff():
    print("需要清空的数据:" + str(pgsql_delete))
    # 建立数据库连接
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_pass, host=db_host, port=db_port
    )

    try:
        # 创建一个cursor对象用于执行SQL语句
        cur = conn.cursor()

        # SQL查询语句
        sql_query = "delete from rskm_pt where insurancenum= %s;"
        # print(insurance_json)

        # 执行SQL查询
        for dele in pgsql_delete:
            cur.execute(sql_query, (dele,))

        # 提交事务
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        # 如果发生错误，则回滚事务
        conn.rollback()

    finally:
        # 关闭cursor和连接
        cur.close()
        conn.close()

        del conn


def ajaxSend():
    timet = str(datetime.now() - timedelta(seconds=5))
    action = "/api/syncinsurance/syncinsdataresult"

    # 定义请求头
    headers = {
        "Content-Type": "application/json",
        "ApiKey": apiKey,
        "Sign": get_md5_sum(
            "apikey={}&time={}&key={}".format(apiKey, timet, privateKey)
        ),
        "Time": timet,
        "Action": action,
    }

    params = {
        "BillList": list_bill,
        "ResultList": pgsql_bj,
    }

    # POST请求
    response = requests.post(
        url, headers=headers, data=json.dumps(params)
    )  # ,json=json.dumps(params)

    # print(json.dumps(params))

    try:
        # 打印响应状态码和内容
        if response.status_code == 200:
            print(response.text)
        else:
            print("请求失败" + response.status_code + response.text)

    except Exception as e:
        print(f"ajaxSend 请求异常: {e}")

    finally:
        del timet
        del action
        del headers
        del response


# md5获取
def get_md5_sum(string):
    md5_hash = hashlib.md5()
    md5_hash.update(string.encode("utf-8"))
    md5_sum = md5_hash.hexdigest()
    return md5_sum

def random_seconds(min_time=3, max_time=10):
    # 使用random.uniform生成[min_time, max_time]之间的随机浮点数
    random_time = random.uniform(min_time, max_time)
    return random_time

# 主入口
def main():
    print(
        f"--------------------------------------------------------------------------------------------"
    )
    print(
        f"0.启动拉取程序----------------------------------------start----------------------------------"
    )
    print(
        f"--------------------------------------------------------------------------------------------"
    )
    # 发起数据拉取请求
    getinsdata()
    print(
        f"1.拉取列表数据----------------------------------------getinsdata-end--------------------------"
    )

    # 清空已存在数据
    queryDiff()
    print(
        f"2.清除历史数据----------------------------------------queryDiff-end--------------------------"
    )

    connPgInstall()
    print(
        f"3.连接提交数据----------------------------------------connPgInstall-end------------------------"
    )

    # # 同步拉取结果
    ajaxSend()
    print(
        f"4.同步拉取结果----------------------------------------ajaxSend-end------------------------"
    )

    # 清空上次内容
    global pgsql_bj
    pgsql_bj = []
    global pgsql
    pgsql = []
    global pgsql_delete
    pgsql_delete = []
    global list_bill
    list_bill = []


if __name__ == "__main__":
    while True:

        main()
        time.sleep(random_seconds())  # 等待10秒
