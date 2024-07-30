from osgeo import ogr
from osgeo import gdal_array
 
# 打开shapefile文件
input_shp = "poi/WPOI.shp"
dataSource = ogr.Open(input_shp)
layer = dataSource.GetLayer()
featureCount = layer.GetFeatureCount()
 
for i in range(10):
    feature = layer.GetNextFeature()
    
    # 获取要素的几何属性
    geometry = feature.GetGeometryRef()
    
    if geometry is not None:
        x = geometry.GetX()
        y = geometry.GetY()
        
        print("第{}个点的坐标为 ({}".format(i+1,geometry.ExportToWkt()))
    
    feature.Destroy()
 
dataSource.Destroy()