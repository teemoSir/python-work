from flask import Flask, request, jsonify
import geopandas as gpd
import psycopg2

app = Flask(__name__)

# 连接到PostGIS数据库
conn = psycopg2.connect(database="postgres", user="postgres", password="root", host="localhost", port="5432")
cur = conn.cursor()

# 获取表名
cur.execute("SELECT table_name FROM public.table_schema WHERE table_name = 'hainan_road_split'")
table_name = cur.fetchone()[0]

# 定义查询语句
query = f"""
    SELECT * FROM {table_name}
    WHERE ST_AsMVTGeom(geom, 'mvt') IS NOT NULL
"""

# 执行查询
cur.execute(query)

# 获取查询结果
rows = cur.fetchall()

# 关闭数据库连接
cur.close()
conn.close()

# 将查询结果转换为GeoDataFrame
geo_data = gpd.GeoDataFrame(rows, columns=["id", "geom"])
geo_data["geom"] = geo_data["geom"].apply(lambda x: x[0])

# 定义矢量瓦片服务的基本URL模板
vector_tile_service_url_template = "http://localhost/{{ zoom }}/{{ x }}/{{ y }}.mvt"

@app.route("/tile", methods=["GET"])
def tile():
    # 获取请求参数
    zoom = request.args.get("zoom", 13)
    x = request.args.get("x", 0)
    y = request.args.get("y", 0)

    # 构建矢量瓦片服务URL
    vector_tile_service_url = vector_tile_service_url_template.format(zoom=zoom, x=x, y=y)

    # 获取矢量瓦片服务响应
    response = requests.get(vector_tile_service_url)

    # 返回响应
    return response.text

if __name__ == "__main__":
    app.run(host="localhost", port=9000)