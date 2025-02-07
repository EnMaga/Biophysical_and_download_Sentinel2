import datetime as dt
import getpass
import re
import shutil
import os
from pathlib import Path
from typing import Optional, Union

import pyproj
import shapely
import xarray as xr

from tqdm import tqdm
from sentinelhub import (
    BBox,
    CRS,
    DataCollection,
    MimeType,
    SentinelHubCatalog,
    SentinelHubRequest,
    SHConfig,
    bbox_to_dimensions,
)

MIN_FILE_SIZE = 1024 * 1024
Sentinel2Image = dict[str, Union[str, dt.date, Path]]

# Sentinel-2 bands relevant for both biophysical variables and vegetation indices
relevant_bands = [
    "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08",
    "B8A", "B09", "B11", "B12", "sunAzimuthAngles", "sunZenithAngles",
    "viewAzimuthMean", "viewZenithMean",
]

def create_configuration(
    client_id: Optional[str] = None, client_secret: Optional[str] = None
) -> SHConfig:
    """
    Creates a SentinelHub configuration object.

    Args:
        client_id (str, optional): SentinelHub client ID.
        client_secret (str, optional): SentinelHub client secret.

    Returns:
        SHConfig: Configured SentinelHub object.
    """
    config = SHConfig()
    config.sh_token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    config.sh_base_url = "https://sh.dataspace.copernicus.eu"

    if client_id and client_secret:
        config.sh_client_id = client_id
        config.sh_client_secret = client_secret
    elif not config.sh_client_id and not config.sh_client_secret:
        config.sh_client_id = getpass.getpass("Enter your SentinelHub client id")
        config.sh_client_secret = getpass.getpass("Enter your SentinelHub client secret")

    return config


def generate_evalscript() -> str:
    """
    Generates an Evalscript for downloading Sentinel-2 images.

    Returns:
        str: Evalscript string.
    """
    return f"""
//VERSION=3
function setup() {{
    return {{
        input: {str(relevant_bands)},
        output: {{
            bands: {len(relevant_bands)},
            sampleType: "FLOAT32"
        }},
        processing: {{
            upsampling: "BILINEAR"
        }}
    }};
}}

function evaluatePixel(sample) {{
    return [
        {', '.join([f'sample.{band}' for band in relevant_bands])}
    ];
}}
"""


def query_copernicushub(
    config: SHConfig, bbox: BBox, timeframe: tuple[dt.date]
) -> list[Sentinel2Image]:
    """
    Queries the Copernicus Hub to find available Sentinel-2 images.

    Args:
        config (SHConfig): SentinelHub configuration object.
        bbox (BBox): The bounding box of the area of interest.
        timeframe (tuple[dt.date]): The start and end date for searching images.

    Returns:
        list[Sentinel2Image]: A list of available Sentinel-2 images.
    """
    catalog = SentinelHubCatalog(config=config)
    search_results = list(
        catalog.search(
            DataCollection.SENTINEL2_L2A,
            bbox=bbox,
            time=timeframe,
            fields={"include": ["id", "properties.datetime"], "exclude": []},
        )
    )

    return get_recordings(search_results)


def get_recordings(query_result: list[dict[str, str]]) -> list[Sentinel2Image]:
    """
    Processes query results and extracts relevant Sentinel-2 image metadata.

    Args:
        query_result (list): List of queried Sentinel-2 image metadata.

    Returns:
        list[Sentinel2Image]: Processed list of Sentinel-2 images.
    """
    recordings = [
        {"id": res["id"], "date": parse_date(res["properties"]["datetime"])}
        for res in query_result
    ]

    unique_dates = {item["date"] for item in recordings}

    result = []

    for date in unique_dates:
        ids = [recording["id"] for recording in recordings if recording["date"] == date]
        name = generate_name(ids)

        result.append({"name": name, "date": date})

    return result


def parse_date(time: str) -> dt.date:
    """
    Parses a datetime string and converts it to a date.

    Args:
        time (str): Datetime string.

    Returns:
        dt.date: Extracted date.
    """
    # Try parsing with microseconds
    try:
        pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
        parsed_time: dt.datetime = dt.datetime.strptime(time, pattern)
    except ValueError:
        # Fallback to parsing without microseconds
        pattern = "%Y-%m-%dT%H:%M:%SZ"
        parsed_time: dt.datetime = dt.datetime.strptime(time, pattern)

    return parsed_time.date()



def generate_name(ids: list[str]) -> str:
    """
    Generates a name for the Sentinel-2 recording based on its IDs.

    Args:
        ids (list[str]): List of Sentinel-2 IDs.

    Returns:
        str: Generated filename.
    """
    substrings = [re.split(r"[_\.]", text) for text in ids]

    extract_element = lambda index: "".join(
        list({substring[index] for substring in substrings})
    )

    return "_".join(
        [
            extract_element(0),  # Satellite Name
            extract_element(1),  # Processing Level
            extract_element(2),  # Date
            extract_element(3),  # Processing Baseline
            extract_element(4),  # Relative Orbit
        ]
    )


def download_single_satellite_image(
    config: SHConfig,
    bbox: BBox,
    time_interval: tuple[dt.date],
    resulting_file_path: Path,
    temporary_folder: Path = Path("tmp"),
    resolution: int = 60,
    max_cloud_coverage: float = 0.2,
) -> None:
    """
    Downloads a single Sentinel-2 image from the Sentinel Hub API.

    Args:
        config: SentinelHub configuration.
        bbox: Bounding box of the area of interest.
        time_interval: Time range for the requested image.
        resulting_file_path: Path where the image will be saved.
        temporary_folder: Temporary storage folder.
        resolution: Resolution of the downloaded image.
        max_cloud_coverage: Maximum allowed cloud coverage.
    """
    temporary_folder.mkdir(exist_ok=True)

    evalscript = generate_evalscript()

    request = SentinelHubRequest(
        evalscript=evalscript,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL2_L2A.define_from(
                    "s2", service_url=config.sh_base_url
                ),
                time_interval=sorted(
                    tuple(map(lambda x: x.strftime("%Y-%m-%d"), time_interval))
                ),
                maxcc=max_cloud_coverage,
                mosaicking_order="leastCC",
            )
        ],
        responses=[SentinelHubRequest.output_response("default", MimeType.TIFF)],
        bbox=bbox,
        size=bbox_to_dimensions(bbox, resolution=resolution),
        config=config,
        data_folder=str(temporary_folder.absolute()),
    )

    request.get_data(save_data=True)
    tmp_file_path = Path(temporary_folder) / Path(request.get_filename_list()[0])

    file_size = os.path.getsize(tmp_file_path)
    if file_size < MIN_FILE_SIZE:
        raise RuntimeError("File too small")

    tmp_file_path.rename(resulting_file_path)


def load_satellite_images(
    config: SHConfig,
    aoi: dict[str, Union[shapely.box, pyproj.crs.crs.CRS]],
    time_range: tuple[dt.date],
    file_path: Path,
    show_progress: bool = False,
    tmp_dir: Path = Path("tmp"),
) -> list[Sentinel2Image]:
    """
    Loads available Sentinel-2 images for a given area and time range.

    Args:
        config: SentinelHub configuration.
        aoi: Dictionary containing bounding box and coordinate reference system.
        time_range: Time range for the requested images.
        file_path: Path where the images will be stored.
        show_progress: Whether to display a progress bar.
        tmp_dir: Temporary directory for downloads.

    Returns:
        list[Sentinel2Image]: List of downloaded Sentinel-2 images.
    """
    bbox = BBox(aoi["bbox"], aoi["crs"])

    # Retrieve available images
    available_recordings = query_copernicushub(config, bbox, time_range)

    # Ensure file_path directory exists
    file_path.mkdir(exist_ok=True)
    
    tmp_exists = tmp_dir.is_dir()
    tmp_dir.mkdir(exist_ok=True)

    # Add "path" key to each image dictionary
    for item in available_recordings:
        item["path"] = file_path / f"{item['name']}.tif"

    downloaded = []
    for item in tqdm(available_recordings, unit="image", disable=not show_progress):
        if not item["path"].is_file():
            delta = dt.timedelta(days=1)
            time_interval = (item["date"] - delta, item["date"] + delta)
            try:
                download_single_satellite_image(
                    config, bbox, time_interval, item["path"]
                )
            except Exception as e:
                print(f"Failed to download {item['name']}: {e}")
                continue
        downloaded.append(item)

    # Remove temporary directory if it was created in this function
    if not tmp_exists:
        shutil.rmtree(tmp_dir)

    return sorted(downloaded, key=lambda x: x["date"])

