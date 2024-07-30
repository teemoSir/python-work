import geopandas as gpd
from osgeo import ogr
 
# 定义输入PBF文件路径
input_path = "hainan-latest.osm.pbf"
 
# 创建OGR数据源对象并打开PBF文件
driver = ogr.GetDriverByName("OSM")
dataSource = driver.Open(input_path)
layer = dataSource.GetLayer()
 
# 获取图层属性信息
fieldDefnList = layer.schema['properties']
fields = [defn.name for defn in fieldDefnList]
 
# 遍历每个要素并提取相关字段值
features = []
for feature in layer:
    attributes = {}
    for field in fields:
        value = feature[field].encode('utf-8') if isinstance(feature[field], bytes) else str(feature[field])
        attributes[field] = value
    
    # 添加到特征列表
    features.append({'geometry': feature.geometry(), 'attributes': attributes})
 
# 创建GeoDataFrame对象
gdf = gpd.GeoDataFrame(features)
 
# 指定输出SHP文件路径
output_path = "hainan_osm.shp"
 
# 导出为SHP文件
gdf.to_file(output_path, driver='ESRI Shapefile', encoding="UTF-8", index=False)
 
print("转换完成！")