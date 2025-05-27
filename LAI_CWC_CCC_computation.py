"""
python /home/enrico/s2_cuts/s2_cuts/LAI_CCC_CWC_computation.py \
  --geojson your/path/tofile.geojson \
  --start_date 2019-01-01 \
  --end_date   2019-02-01 \
  --out_dir    your/path/to/output \
  --target_crs EPSG:32632

"""

import os
import time
os.environ["GDAL_NUM_THREADS"] = "ALL_CPUS"

import argparse
import gc
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

import geopandas as gpd
import pandas as pd
import numpy as np
import satellitetools as sattools
from satellitetools.common.sentinel2 import S2Band
import satellitetools.aws as aws_mod
import rasterio
from rasterio.io import MemoryFile
from skimage.morphology import remove_small_objects, closing, square
from scipy.ndimage import binary_fill_holes
import rioxarray

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# ----------------------------------------------------------------
# Monkey-patch: force L2A and dedupe granules by highest index
# ----------------------------------------------------------------

def _patched_search_s2_items(self):
    logger = logging.getLogger()
    logger.info(
        f"Searching S2 L2A data from {self.req_params.datestart} to {self.req_params.dateend}"
    )
    bbox = list(self.aoi.geometry.bounds)
    items = aws_mod.EarthSearch(
        datestart=self.req_params.datestart,
        dateend=self.req_params.dateend,
        bbox=bbox,
        collection=aws_mod.EarthSearchCollection.SENTINEL2_L2A,
    ).get_items()

    unique = {}
    for raw in items:
        raw_id = raw['id'] if isinstance(raw, dict) else raw.id
        parts = raw_id.split('_')
        base_key = '_'.join(parts[0:3] + [parts[-1]])
        try:
            idx = int(parts[3])
        except Exception:
            idx = 0
        prev = unique.get(base_key)
        if prev is None or idx > prev[1]:
            unique[base_key] = (raw, idx)

    deduped = [item for item, _ in unique.values()]
    self.s2_items = [aws_mod.AWSSentinel2Item(item) for item in deduped]
    self.sort_s2_items()

aws_mod.AWSSentinel2DataCollection.search_s2_items = _patched_search_s2_items

#cloud mask based on SCL of sentinel 2 (modify based on your needs)
def process_cloud_mask(scl_array, min_size1=49, min_size2=47):
    mask = np.isin(scl_array, [2, 4, 5, 6, 7])
    mask = remove_small_objects(mask, min_size=min_size1)
    mask = binary_fill_holes(mask)
    mask = closing(mask, square(3))
    mask = remove_small_objects(mask, min_size=min_size2)
    return mask.astype(bool)


def process_date(t, ds_ms, ds_scl, target_crs):
    """
    Compute LAI, chlorophyll (Cab) and water content (Cw) for one timestamp.
    Returns a dict of arrays and profiles for each variable.
    """
    date_str = pd.to_datetime(t).strftime('%Y%m%d')

    # Run SNAP biophysical models at 20m
    ds_lai = sattools.biophys.run_snap_biophys(ds_ms, sattools.biophys.BiophysVariable.LAI)
    ds_cab = sattools.biophys.run_snap_biophys(ds_ms, sattools.biophys.BiophysVariable.LAI_Cab)
    ds_cw  = sattools.biophys.run_snap_biophys(ds_ms, sattools.biophys.BiophysVariable.LAI_Cw)

    # Extract and reproject
    da_lai = ds_lai.sel(time=t)['lai'].squeeze().rio.write_crs(target_crs).rio.reproject(target_crs)
    da_cab = ds_cab.sel(time=t)['lai_cab'].squeeze().rio.write_crs(target_crs).rio.reproject(target_crs)
    da_cw  = ds_cw.sel(time=t)['lai_cw'].squeeze().rio.write_crs(target_crs).rio.reproject(target_crs)

    # Cloud mask (SCL) at 20m
    scl20 = ds_scl.sel(time=t)['SCL'].squeeze().rio.write_crs(target_crs).rio.reproject_match(da_lai)
    mask20 = process_cloud_mask(scl20.values)

    segments = {}
    for var, da in [('LAI', da_lai), ('CCC', da_cab), ('CWC', da_cw)]:
        da_masked = da.where(mask20, -9999).rio.write_nodata(-9999)
        arr = da_masked.values[np.newaxis, ...]
        tf  = da_masked.rio.transform()
        prof = dict(
            driver='GTiff', height=da_masked.shape[0], width=da_masked.shape[1], count=1,
            dtype=da_masked.dtype, crs=da_masked.rio.crs, transform=tf, nodata=-9999
        )
        segments.setdefault(var, {}).setdefault(date_str, []).append((arr, tf, prof))

    return date_str, segments


def process_aoi(geom, start_date, end_date, target_crs):
    # Prepare Sentinel-2 request
    aoi = sattools.AOI(None, geom, 'EPSG:4326')
    bands = S2Band.get_10m_to_20m_bands()
    req_ms  = sattools.Sentinel2RequestParams(start_date, end_date, sattools.DataSource.AWS, bands)
    req_scl = sattools.Sentinel2RequestParams(start_date, end_date, sattools.DataSource.AWS, [S2Band.SCL])

    # Fetch MS and SCL datasets
    _, ds_ms  = sattools.wrappers.get_s2_qi_and_data(aoi=aoi, req_params=req_ms,  qi_threshold=0., qi_filter=[])
    _, ds_scl = sattools.wrappers.get_s2_qi_and_data(aoi=aoi, req_params=req_scl, qi_threshold=0., qi_filter=[])
    if ds_ms is None or ds_scl is None:
        logging.info("No Sentinel-2 data returned for AOI and date range.")
        return None, {}

    # Determine satellite letter
    product_uri = next(
        (v for v in ds_ms.attrs.values() if isinstance(v, str) and v.startswith(('S2A','S2B'))),
        None
    )
    letter = 'A' if product_uri and product_uri.startswith('S2A') else (
             'B' if product_uri and product_uri.startswith('S2B') else 'X')

    # Process each timestamp in parallel
    segments = {}
    times = list(ds_ms.time.values)
    with ProcessPoolExecutor(max_workers=max(1, os.cpu_count()-2)) as executor:
        futures = {executor.submit(process_date, t, ds_ms, ds_scl, target_crs): t for t in times}
        for fut in as_completed(futures):
            date_str, segs = fut.result()
            for var, var_segs in segs.items():
                for d, lst in var_segs.items():
                    segments.setdefault(var, {}).setdefault(d, []).extend(lst)

    return letter, segments


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test_id',   type=str)
    parser.add_argument('--geojson',   type=str, required=True)
    parser.add_argument('--out_dir',   type=str, default='output')
    parser.add_argument('--start_date',type=str, default='2019-01-01')
    parser.add_argument('--end_date',  type=str, default='2025-01-01')
    parser.add_argument('--target_crs',type=str, default='EPSG:32632')
    args = parser.parse_args()

    start_time = time.time()

    os.makedirs(args.out_dir, exist_ok=True)
    gdf  = gpd.read_file(args.geojson).to_crs('EPSG:4326')
    if args.test_id:
        gdf = gdf[gdf['id'] == args.test_id]
    geom  = gdf.iloc[0].geometry
    aoi_id = args.test_id or str(gdf.iloc[0].get('id', 'aoi'))

    sat_letter, segments = process_aoi(geom, args.start_date, args.end_date, args.target_crs)
    if sat_letter is None:
        return

    # Write outputs locally (no mosaic needed)
    for var, var_mosaic in segments.items():
        for date_str, seg_list in var_mosaic.items():
            # Only one segment per var-date since no tiling
            arr, tf, prof = seg_list[0]
            outdir = os.path.join(args.out_dir, f'index={var}', f'aoi={aoi_id}')
            os.makedirs(outdir, exist_ok=True)
            # Filename without satellite letter
            filename = f'S2_{date_str}_000_{aoi_id}_{var}.tif'
            local_path = os.path.join(outdir, filename)

            # Update to COG profile
            prof.update(
                driver='COG', height=arr.shape[1], width=arr.shape[2],
                transform=tf, compress='LZW', blocksize=512,
                overview_resampling='nearest', overview_blocksize=512,
                overviews='AUTO'
            )
            with rasterio.open(local_path, 'w', **prof) as dst:
                dst.write(arr)
            logging.info(f"Saved {filename} to {local_path}")

    total_time = time.time() - start_time
    logging.info(f"Total processing time: {total_time:.2f} seconds")

if __name__ == '__main__':
    main()
