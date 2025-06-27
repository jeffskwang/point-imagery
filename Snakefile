import pandas as pd

# read in points of interest
poi = pd.read_csv("in/points_of_interest.csv")

# define bands to download
target_asset_keys = ["B04", "B03", "B02"]

# define radius around points
radius = 100.0

# make directories
if not os.path.exists("out"):
    os.makedirs("out")
if not os.path.exists("tmp"):
    os.makedirs("tmp")

rule all:
    input:
        ["out/"+ poi["name"][i] + "_"+ str(poi["lat"][i]) + "_"+ str(poi["lon"][i]) + "_"+ str(radius) + "m.tif" for i in range(0,len(poi))]

for i in range(0,len(poi)):
    rule:
        name:
            "download_rgb_" + poi["name"][i]
        params:
            name = poi["name"][i],
            lat = poi["lat"][i],
            lon = poi["lon"][i],
            radius = radius,
            collection_id = "sentinel-2-l2a",
            target_asset_keys = target_asset_keys,
            query = {
                    "platform":       {"in": ["Sentinel-2A","Sentinel-2B"]}
                    },
            start_query = "2024-05-01",
            end_query = "2024-05-30"
        output:
            out_bands = ["tmp/"
                        + poi["name"][i] + "_"
                        + str(poi["lat"][i]) + "_"
                        + str(poi["lon"][i]) + "_"
                        + str(radius) + "m_"
                        + target_asset_key + ".tif"
                        for target_asset_key in target_asset_keys]
        script:
            "src/get_imagery.py"
    
    rule:
        name:
            "marge_" + poi["name"][i]
        input:
            rgb_bands= ["tmp/"
                        + poi["name"][i] + "_"
                        + str(poi["lat"][i]) + "_"
                        + str(poi["lon"][i]) + "_"
                        + str(radius) + "m_"
                        + target_asset_key + ".tif"
                        for target_asset_key in target_asset_keys]
        output:
            out_tif = "out/"
                        + poi["name"][i] + "_"
                        + str(poi["lat"][i]) + "_"
                        + str(poi["lon"][i]) + "_"
                        + str(radius) + "m.tif"
        shell:
            "gdal_merge.py -separate -o {output.out_tif} -co PHOTOMETRIC=RGB {input[0]} {input[1]} {input[2]}"
