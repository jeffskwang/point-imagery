import os
import time
import shutil
import subprocess
from pystac_client import Client
import requests
from typing import List
from typing import Tuple
import shapely
from shapely.geometry import box, shape, mapping
import numpy as np
from osgeo import gdal
import osgeo_utils.gdal_merge
import geopandas as gpd
import urllib.parse
from planetary_computer import sign_url
import pandas as pd


def query_imagery_stac(
    intersects: dict = None,
    datetime: str = None,
    collections: List[str] = None,
    query: dict = None,
    max_items: int = None,
) -> List[dict]:
    """Queries the STAC server for data with the following input parameters and returns a list of scenes

    Parameters
    ----------
    intersects: GeoJSON object
        A GeoJSON object representing the geographical area of interest
    daterange: string
        A string specifying the date range for the query in the format 'YYYY-MM-DD/YYYY-MM-DD'
    collections: string of list of strings
        A string or a list of strings specifying the collections to search. Data found here: https://planetarycomputer.microsoft.com/catalog
    query: dictionary
        dictionary for additional query parameters
    max_items: integer
        An integer specifying the maximum number of items to return

    Returns
    -------
    query: list
        list of scenes from the stac search

    """

    stac = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    if intersects or datetime or collections or query is not None:
        query = stac.search(
            collections=collections,
            max_items=max_items,
            intersects=intersects,
            datetime=datetime,
            query=query,
        )
        return query
    else:
        return print(
            "Must set at least one of the following parameters: collections, intersects, datetime or query before continuing."
        )


def calculate_coverage_ratio(scene_geom, aoi_geom):
    """Compute what area of the bounding box geometry is covered by the scene geometry.

    Parameters
    ----------
    scene_geom: geometry
        geometry of the scene
    aoi_geom: geometry
        geometry of the remaining area that isn't covered by a scene

    Returns
    -------
    proportion: float
        proportion of the area in the aoi_geometry that is covered by the scene

    """

    intersection = scene_geom.intersection(aoi_geom)

    if aoi_geom.area == 0.0:
        return 1.0
    else:
        return intersection.area / aoi_geom.area


def download_scenes(extent, item, target_asset_key, output_file):
    """Download all the scenes in the extent with gdalwrap

    Parameters
    ----------
    extent: list
        list of minx, miny, maxx, maxy values of the bounding box
    item: scene items
        scene item to download
    target_asset_key: string
        name of target asset to download, e.g., band to download. You'll need to look up the names of the assets
        for the particular collection: https://planetarycomputer.microsoft.com/catalog
    output_file: string
        path of the image output

    Returns
    -------
    none

    """

    # initialize a list of the stac urls to be downloaded
    for asset_key, asset in item.assets.items():
        # Check if the asset is one of the desired bands
        if asset_key == target_asset_key:
            stac_url = sign_url(asset.href)
            
    # GDAL command
    command = (
        [
            "gdalwarp",
            "-te",
            str(extent[0]),
            str(extent[1]),
            str(extent[2]),
            str(extent[3]),
            "-t_srs",
            "EPSG:4326",
        ]
        + ["/vsicurl/" + stac_url]
        + [output_file]
    )
    # Run the command
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Check for errors
    if result.returncode != 0:
        print("Error running gdalwarp:", result.stderr.decode())
    else:
        print("gdalwarp completed successfully:", result.stdout.decode())


def get_imagery(
    out_dir,
    lat,
    lon,
    radius,
    collection_id,
    query,
    target_asset_keys,
    start_query,
    end_query,
    prefix
):
    """Gets imagery for a from PlanetaryComputer

    Parameters
    ----------
    out_dir: string
        path where files will be downloaded
    lat: float
        latitude of point
    lon: float
        longitude of point
    radius:
        radius in meters around point
    collection_id: string
        name of the collection, see here: https://planetarycomputer.microsoft.com/catalog
    target_asset_keys: list of strings
        name of target asset to download, e.g., band to download. You'll need to look up the names of the assets
        for the particular collection: https://planetarycomputer.microsoft.com/catalog
    query: dictionary
        dictionary used to filter the query search
    start_query: datetime
        YYYY-MM-DD date that brackets the start the search. You can also add time in this format: YYYY-MM-DDTHH:MM:SSZ"
    start_query: datetime
        YYYY-MM-DD date that brackets the end the search. You can also add time in this format: YYYY-MM-DDTHH:MM:SSZ"

    Returns
    -------
    none

    """

    # make out_dir
    df = pd.DataFrame({'longitude': [lon], 'latitude': [lat]})
    geometry = gpd.points_from_xy(df.longitude, df.latitude, crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    utm_crs = gdf.estimate_utm_crs()
    gdf_buff = gdf.to_crs(utm_crs).buffer(radius)

    bounding_circle_geom = gdf_buff.to_crs("EPSG:4326").geometry.iloc[0]
    
    # Set intersects using polygon geojson
    geojson_obj = mapping(bounding_circle_geom)

    # query the stac api to scene matches
    query_return = query_imagery_stac(
        collections=[collection_id],
        query=query,
        intersects=geojson_obj,
        datetime=start_query + "/" + end_query,
    )
    
    # Store the items in a list
    items = list(query_return.item_collection())

    # Filter out items with banding issues
    # Assuming 'banding_issues' is a flag in the properties (this will depend on the dataset)
    QA_items = [
        item
        for item in items
        if "banding_issues" not in item.properties
        or item.properties["banding_issues"] != True
    ]

    # choose first item - THIS NEEDS TO BE UPDATED OR THE QUERY TO GET WHAT WE WANT
    chosen_item = QA_items[0]
    
    # download the scenes for each target asset in the collection
    for target_asset_key in target_asset_keys:
        download_scenes(
            list(gdf_buff.to_crs("EPSG:4326").geometry.total_bounds),
            chosen_item,
            target_asset_key,
            out_dir + "/" + prefix + target_asset_key + ".tif",
        )


if __name__ == "__main__":
    name = snakemake.params["name"]
    lat = snakemake.params["lat"]
    lon = snakemake.params["lon"]
    radius = snakemake.params["radius"]
    collection_id = snakemake.params["collection_id"]
    query = snakemake.params["query"]
    target_asset_keys = snakemake.params["target_asset_keys"]
    start_query = snakemake.params["start_query"]
    end_query = snakemake.params["end_query"]
    out_bands = snakemake.output["out_bands"]
    out_dir = os.path.dirname(out_bands[0])

    get_imagery(
        out_dir,
        lat,
        lon,
        radius,
        collection_id,
        query,
        target_asset_keys,
        start_query,
        end_query,
        name + "_" + str(lat) + "_" + str(lon) +"_" + str(radius) + "m_"
    )
