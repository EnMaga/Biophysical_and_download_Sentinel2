#!/usr/bin/env python3
"""
FINAL_extraction_S2.py

Sentinel-2 extraction script that:
- downloads Sentinel-2 L2A data from AWS through satellitetools/EarthSearch
- prints when AWS tiles are found for each day
- saves only processed raster outputs in folders

Outputs:
- biophysical: LAI, CCC, CWC
- bands: B2, B3, B4, B5, B6, B7, B8, B8A, B11, B12
- indices: NDVI, NDWI, NDII, MSAVI2, CIRE, EVI, NDRE1, MTCI

Usage:
cd /folder/where/script/is.py

python download_processing_S2.py \
  --aoi_label YOUR_AOI \
  --start_date YYYY-MM-DD \
  --end_date YYYY-MM-DD \
  --geojson /path/to/your/AOI.geojson \
  --out_dir /path/to/folder/where/to/save/outputs

"""

import os
os.environ["GDAL_NUM_THREADS"] = "ALL_CPUS"

import argparse
import gc
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import datetime
import sys
import traceback
import warnings
import logging

warnings.simplefilter(action="ignore", category=FutureWarning)

import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
import rioxarray
from shapely import wkt as shapely_wkt
from skimage.morphology import remove_small_objects, closing, square
from scipy.ndimage import binary_fill_holes

import satellitetools as sattools
from satellitetools.common.sentinel2 import S2Band
import satellitetools.aws as aws_mod


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("FINAL_extraction_S2")


# ------------------------------------------------------------
# Multiprocessing start method
# ------------------------------------------------------------
try:
    multiprocessing.set_start_method("spawn", force=True)
except RuntimeError:
    pass


# ------------------------------------------------------------
# Patch EarthSearch: force L2A and keep only max granule index
# Also prints when AWS tiles are found
# ------------------------------------------------------------
def _patched_search_s2_items(self):
    bbox = list(self.aoi.geometry.bounds)

    logger.info(
        f"[AWS SEARCH] AOI={getattr(self.aoi, 'name', 'AOI')} | "
        f"date_range={self.req_params.datestart} -> {self.req_params.dateend} | "
        f"bbox={bbox}"
    )

    items = aws_mod.EarthSearch(
        datestart=self.req_params.datestart,
        dateend=self.req_params.dateend,
        bbox=bbox,
        collection=aws_mod.EarthSearchCollection.SENTINEL2_L2A,
    ).get_items()

    logger.info(f"[AWS SEARCH] raw items found: {len(items)}")

    unique = {}
    for raw in items:
        raw_id = raw["id"] if isinstance(raw, dict) else raw.id
        parts = raw_id.split("_")
        base_key = "_".join(parts[0:3] + [parts[-1]])

        try:
            idx = int(parts[3])
        except Exception:
            idx = 0

        logger.info(f"[AWS TILE FOUND] {raw_id}")

        prev = unique.get(base_key)
        if prev is None or idx > prev[1]:
            unique[base_key] = (raw, idx)

    deduped = [item for item, _ in unique.values()]
    logger.info(f"[AWS SEARCH] deduped items kept: {len(deduped)}")

    for item in deduped:
        item_id = item["id"] if isinstance(item, dict) else item.id
        logger.info(f"[AWS TILE KEPT] {item_id}")

    self.s2_items = [aws_mod.AWSSentinel2Item(item) for item in deduped]
    self.sort_s2_items()


aws_mod.AWSSentinel2DataCollection.search_s2_items = _patched_search_s2_items


# ------------------------------------------------------------
# Cloud/valid mask from SCL
# ------------------------------------------------------------
def process_cloud_mask(scl_array, min_size1=49, min_size2=47):
    """
    Build validity mask from SCL.
    Kept classes: 2, 4, 5, 6, 7
    """
    mask = np.isin(scl_array, [2, 4, 5, 6, 7])
    mask = remove_small_objects(mask, min_size=min_size1)
    mask = binary_fill_holes(mask)
    mask = closing(mask, square(3))
    mask = remove_small_objects(mask, min_size=min_size2)
    return mask.astype(bool)


# ------------------------------------------------------------
# Daily date windows [d, d+1)
# ------------------------------------------------------------
def _daily_range(start_date: str, end_date: str):
    for d in pd.date_range(start=start_date, end=end_date, freq="D"):
        sd = d.strftime("%Y-%m-%d")
        ed = (d + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        yield sd, ed


# ------------------------------------------------------------
# UTM CRS from AOI centroid
# ------------------------------------------------------------
def _utm_crs_from_geom(geom):
    centroid = geom.centroid
    lon, lat = centroid.x, centroid.y
    zone = int((lon + 180) / 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    return f"EPSG:{epsg}"


# ------------------------------------------------------------
# DN -> reflectance
# ------------------------------------------------------------
def _to_reflectance(da):
    da = da.astype("float32")
    try:
        vmax = float(da.max().values)
    except Exception:
        vmax = float(da.max())
    if vmax > 2.0:
        return da / 10000.0
    return da


# ------------------------------------------------------------
# Worker: process one day
# ------------------------------------------------------------
def process_day(aoi_wkt, aoi_label, sd, ed, target_crs, out_dir, skip_existing=True):
    t0_day = time.time()

    geom = shapely_wkt.loads(aoi_wkt)
    aoi = sattools.AOI(aoi_label, geom, "EPSG:4326")
    bands = S2Band.get_10m_to_20m_bands()

    logger.info(f"[day {sd}] starting")

    # ----------------------------------------
    # Load data from AWS
    # ----------------------------------------
    req_ms = sattools.Sentinel2RequestParams(
        sd, ed, sattools.DataSource.AWS, bands
    )
    req_scl = sattools.Sentinel2RequestParams(
        sd, ed, sattools.DataSource.AWS, [S2Band.SCL]
    )

    try:
        t0 = time.time()
        df_q, ds_ms = sattools.wrappers.get_s2_qi_and_data(
            aoi=aoi,
            req_params=req_ms,
            qi_threshold=0.0,
            qi_filter=[]
        )
        _, ds_scl = sattools.wrappers.get_s2_qi_and_data(
            aoi=aoi,
            req_params=req_scl,
            qi_threshold=0.0,
            qi_filter=[]
        )
        logger.info(f"[day {sd}] AWS load done in {time.time() - t0:.1f}s")
    except Exception as e:
        logger.warning(f"[day {sd}] load failed: {e}")
        return sd

    if ds_ms is None or ds_scl is None:
        logger.info(f"[day {sd}] no Sentinel-2 tile found on AWS")
        return sd
    else:
        logger.info(f"[day {sd}] Sentinel-2 tile found on AWS")

    # ----------------------------------------
    # Satellite letter
    # ----------------------------------------
    sat_letter = "X"
    product_uri = None
    for _, val in ds_ms.attrs.items():
        if isinstance(val, str) and val.startswith(("S2A", "S2B", "S2C", "S2D")):
            product_uri = val
            break

    if product_uri is None and df_q is not None and not df_q.empty:
        for col in df_q.columns:
            if "product" in col.lower():
                val = df_q.iloc[0][col]
                if isinstance(val, str) and val.startswith(("S2A", "S2B", "S2C", "S2D")):
                    product_uri = val
                    break

    if product_uri:
        sat_letter = product_uri[2]

    # ----------------------------------------
    # Biophysical
    # ----------------------------------------
    try:
        t0 = time.time()
        ds_lai = sattools.biophys.run_snap_biophys(
            ds_ms, sattools.biophys.BiophysVariable.LAI
        )
        ds_ccc = sattools.biophys.run_snap_biophys(
            ds_ms, sattools.biophys.BiophysVariable.LAI_Cab
        )
        ds_cwc = sattools.biophys.run_snap_biophys(
            ds_ms, sattools.biophys.BiophysVariable.LAI_Cw
        )
        logger.info(f"[day {sd}] biophysical phase done in {time.time() - t0:.1f}s")
    except Exception as e:
        logger.warning(f"[day {sd}] biophysical failed: {e}")
        ds_lai = ds_ccc = ds_cwc = None

    times = ds_ms.time.values if hasattr(ds_ms, "time") else [None]

    for t in times:
        date_val = pd.to_datetime(t) if t is not None else pd.to_datetime(sd)
        date_str = date_val.strftime("%Y%m%d")
        logger.info(f"[day {sd}] processing slice {date_str}")

        ds_t = ds_ms.sel(time=t)

        def _get_band(ds, band_name):
            if "band_data" in ds.data_vars and "band" in ds.coords:
                da_b = ds["band_data"]
                band_vals = [str(b) for b in da_b["band"].values]
                if band_name in band_vals:
                    idx = band_vals.index(band_name)
                    return da_b.isel(band=idx).squeeze()
                raise KeyError(f"Band {band_name} not found. Available: {band_vals}")

            if band_name in ds.data_vars:
                return ds[band_name].squeeze()

            raise KeyError(
                f"Band {band_name} not found. "
                f"Data variables: {list(ds.data_vars)} | coords: {list(ds.coords)}"
            )

        # ----------------------------------------
        # Reference 10m grid
        # ----------------------------------------
        try:
            ref_10m = _get_band(ds_t, "B8")
        except KeyError:
            ref_10m = _get_band(ds_t, "B4")

        ref_10m = ref_10m.rio.write_crs(target_crs).rio.reproject(
            target_crs,
            resolution=10
        )

        # ----------------------------------------
        # SCL -> 10m mask
        # ----------------------------------------
        scl = ds_scl.sel(time=t)["SCL"].squeeze()
        scl = scl.rio.write_crs(target_crs).rio.reproject_match(ref_10m)
        mask = process_cloud_mask(scl.values)

        tf = ref_10m.rio.transform()
        base_crs = ref_10m.rio.crs

        base_prof = {
            "driver": "COG",
            "height": ref_10m.shape[0],
            "width": ref_10m.shape[1],
            "count": 1,
            "dtype": "float32",
            "crs": base_crs,
            "transform": tf,
            "nodata": -9999,
            "compress": "DEFLATE",
            "zlevel": 9,
            "blocksize": 512,
            "overviews": "AUTO",
            "overview_blocksize": 512,
            "overview_resampling": "nearest",
            "bigtiff": "IF_SAFER",
        }

        def _write_cog(da_expr, index_name):
            local_outdir = os.path.join(out_dir, f"index={index_name}", f"aoi={aoi_label}")
            os.makedirs(local_outdir, exist_ok=True)

            filename = f"S2_{date_str}_000_{aoi_label}_{sat_letter}_{index_name}.tif"
            local_outpath = os.path.join(local_outdir, filename)

            if skip_existing and os.path.exists(local_outpath):
                logger.info(f"[day {sd}] [skip] {index_name} already exists")
                return

            try:
                da_expr = da_expr.rio.write_crs(target_crs).rio.reproject_match(ref_10m)
                da_expr = da_expr.where(mask, -9999).rio.write_nodata(-9999)
                arr = da_expr.values[np.newaxis, :, :].astype("float32")

                prof = base_prof.copy()
                prof["dtype"] = "float32"

                with rasterio.open(local_outpath, "w", **prof) as dst:
                    dst.write(arr)

                logger.info(f"[day {sd}] [saved] {local_outpath}")
            except Exception as e:
                logger.warning(f"[day {sd}] [write failed] {index_name}: {e}")

        # ----------------------------------------
        # Write LAI / CCC / CWC
        # ----------------------------------------
        if ds_lai is not None and "lai" in ds_lai:
            da_lai = ds_lai.sel(time=t)["lai"].squeeze()
            if da_lai.size != 0:
                _write_cog(da_lai, "LAI")

        if ds_ccc is not None and "lai_cab" in ds_ccc:
            da_ccc = ds_ccc.sel(time=t)["lai_cab"].squeeze()
            _write_cog(da_ccc, "CCC")

        if ds_cwc is not None and "lai_cw" in ds_cwc:
            da_cwc = ds_cwc.sel(time=t)["lai_cw"].squeeze()
            _write_cog(da_cwc, "CWC")

        # ----------------------------------------
        # Write pure bands + indices
        # ----------------------------------------
        try:
            t0 = time.time()

            BLUE  = _to_reflectance(_get_band(ds_t, "B2"))
            GREEN = _to_reflectance(_get_band(ds_t, "B3"))
            RED   = _to_reflectance(_get_band(ds_t, "B4"))
            RE1   = _to_reflectance(_get_band(ds_t, "B5"))
            RE2   = _to_reflectance(_get_band(ds_t, "B6"))
            RE3   = _to_reflectance(_get_band(ds_t, "B7"))
            NIR   = _to_reflectance(_get_band(ds_t, "B8"))
            NIRn  = _to_reflectance(_get_band(ds_t, "B8A"))
            SWIR1 = _to_reflectance(_get_band(ds_t, "B11"))
            SWIR2 = _to_reflectance(_get_band(ds_t, "B12"))

            # pure bands
            _write_cog(BLUE,  "B2")
            _write_cog(GREEN, "B3")
            _write_cog(RED,   "B4")
            _write_cog(RE1,   "B5")
            _write_cog(RE2,   "B6")
            _write_cog(RE3,   "B7")
            _write_cog(NIR,   "B8")
            _write_cog(NIRn,  "B8A")
            _write_cog(SWIR1, "B11")
            _write_cog(SWIR2, "B12")

            # indices
            ndvi = (NIR - RED) / (NIR + RED)
            ndwi = (GREEN - NIR) / (GREEN + NIR)
            ndii = (NIR - SWIR1) / (NIR + SWIR1)

            ms_term = (2 * NIR + 1)
            msavi2 = (ms_term - np.sqrt(ms_term ** 2 - 8 * (NIR - RED))) / 2

            cire = (NIR / RE1) - 1
            evi = 2.5 * (NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1)
            ndre1 = (NIRn - RE1) / (NIRn + RE1)
            mtci = (RE2 - RE1) / (RE1 - RED)

            _write_cog(ndvi, "NDVI")
            _write_cog(ndwi, "NDWI")
            _write_cog(ndii, "NDII")
            _write_cog(msavi2, "MSAVI2")
            _write_cog(cire, "CIRE")
            _write_cog(evi, "EVI")
            _write_cog(ndre1, "NDRE1")
            _write_cog(mtci, "MTCI")

            logger.info(f"[day {sd}] spectral phase done in {time.time() - t0:.1f}s")

        except Exception as e:
            logger.warning(f"[day {sd}] spectral indices/bands failed for {date_str}: {e}")

        gc.collect()

    del ds_ms, ds_scl, ds_lai, ds_ccc, ds_cwc
    gc.collect()

    logger.info(f"[day {sd}] done in {time.time() - t0_day:.1f}s")
    return sd


# ------------------------------------------------------------
# Main CLI
# ------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aoi_label", type=str, required=True,
                        help="AOI label used in folder/file names")
    parser.add_argument("--start_date", type=str, required=True,
                        help="Start date YYYY-MM-DD")
    parser.add_argument("--end_date", type=str, required=True,
                        help="End date YYYY-MM-DD")
    parser.add_argument("--geojson", type=str, required=True,
                        help="Path to AOI GeoJSON")
    parser.add_argument("--out_dir", type=str, required=True,
                        help="Output root directory")
    parser.add_argument("--workers", type=int, default=3,
                        help="Number of parallel workers")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing TIFFs instead of skipping")

    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    geojson_path = os.path.expanduser(args.geojson)
    if not os.path.exists(geojson_path):
        logger.error(f"GeoJSON not found: {geojson_path}")
        sys.exit(2)

    gdf = gpd.read_file(geojson_path).to_crs("EPSG:4326")
    if gdf.empty:
        logger.error(f"No features found in AOI GeoJSON: {geojson_path}")
        sys.exit(3)

    row = gdf.iloc[0]
    geom = row.geometry
    if geom is None or geom.is_empty:
        logger.error(f"Geometry of first feature is empty in {geojson_path}")
        sys.exit(3)

    aoi_label = args.aoi_label.strip()
    target_crs = _utm_crs_from_geom(geom)

    logger.info(
        f"+++ Start: {datetime.datetime.now().isoformat()} | "
        f"AOI={aoi_label} | {args.start_date} -> {args.end_date} | "
        f"target_crs={target_crs} +++"
    )

    days = list(_daily_range(args.start_date, args.end_date))
    if not days:
        logger.error("No days in range")
        sys.exit(4)

    aoi_wkt = geom.wkt
    skip_existing = not args.overwrite

    t0_all = time.time()

    if len(days) == 1:
        sd, ed = days[0]
        process_day(aoi_wkt, aoi_label, sd, ed, target_crs, args.out_dir, skip_existing)
    else:
        max_workers = min(args.workers, max(1, os.cpu_count() - 1))
        logger.info(f"[INFO] Parallel processing on {len(days)} days with {max_workers} workers")

        try:
            mp_context = multiprocessing.get_context("spawn")
            with ProcessPoolExecutor(max_workers=max_workers, mp_context=mp_context) as exe:
                futures = {
                    exe.submit(
                        process_day, aoi_wkt, aoi_label, sd, ed, target_crs, args.out_dir, skip_existing
                    ): (sd, ed)
                    for sd, ed in days
                }

                done_count = 0
                for fut in as_completed(futures):
                    sd, ed = futures[fut]
                    try:
                        fut.result()
                        done_count += 1
                        logger.info(f"[PROGRESS] completed {done_count}/{len(days)} days")
                    except Exception as e:
                        logger.error(f"[ERROR] day {sd} failed: {type(e).__name__}: {e}")
                        traceback.print_exc()
                        continue

        except Exception as e:
            logger.error(f"[FATAL] ProcessPoolExecutor failed: {type(e).__name__}: {e}")
            traceback.print_exc()
            sys.exit(5)

    logger.info(f"*** All days done in {time.time() - t0_all:.1f} s ***")


if __name__ == "__main__":
    main()
