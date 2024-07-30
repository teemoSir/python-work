from osgeo import ogr
from osgeo import gdal_array


def do_shpfile(file):
    layer_name = file.split("/")[-1].split(".")[0]
    print(layer_name)
    # if shp_filter(layer_name) is False:
    #   return

    driver = ogr.GetDriverByName('ESRI Shapefile')
    # log(file)

    inds = driver.Open(file, 1)  # 0 - read , 1 - write
    layer = inds.GetLayer()
    print(layer.GetFeatureCount() )
    if layer.GetFeatureCount() == 0:
        return
    gtype = layer.GetLayerDefn().GetGeomType()
    print(gtype)
    if gtype == ogr.wkbLineString:
        # print("=== type:wkbLineString ===")
        do_layerLine(layer)
    if gtype == ogr.wkbPoint:
            print("=== type:wkbPoint ===")
            do_layerPoint(layer)
    # 统一属性秒度转换

    inds.SyncToDisk()
    inds.Destroy()

    del inds
    del layer
    del driver
    del layer_name

def do_layerPoint(layer):
    ftr = layer.ResetReading()
    ftr = layer.GetNextFeature()

    while ftr:
        g = ftr.GetGeometryRef()
        cnt = g.GetPointCount()
        cc = 0
        while cc < cnt:
            # print(g.GetPoint(cc))
            cc += 1

        for n in range(cnt):
            pt = g.GetPoint(n)
            if isinstance(pt[0], float) is False or isinstance(pt[1], float) is False:
                continue
            if pt[0] > 1000 or pt[1] > 1000:
                pta = pt[0]
                ptb = pt[1]

                while pta > 1000:
                    pta = pta / 3600.0
                while ptb > 1000:
                    ptb = ptb / 3600.0
                g.SetPoint(n, round(pta, 6), round(ptb, 6))
        layer.SetFeature(ftr)
        ftr = layer.GetNextFeature()

def do_layerLine(layer):
    ftr = layer.ResetReading()
    ftr = layer.GetNextFeature()

    while ftr:
        g = ftr.GetGeometryRef()
        cnt = g.GetPointCount()
        cc = 0
        while cc < cnt:
            # print(g.GetPoint(cc))
            cc += 1

        for n in range(cnt):
            pt = g.GetPoint(n)
            if isinstance(pt[0], float) is False or isinstance(pt[1], float) is False:
                continue
            if pt[0] > 1000 or pt[1] > 1000:
                pta = pt[0]
                ptb = pt[1]

                while pta > 1000:
                    pta = pta / 3600.0
                while ptb > 1000:
                    ptb = ptb / 3600.0
                g.SetPoint(n, round(pta, 6), round(ptb, 6))
        layer.SetFeature(ftr)
        ftr = layer.GetNextFeature()


if __name__ == '__main__':
    input_shp = "poi/WPOI.shp"
    do_shpfile(input_shp)