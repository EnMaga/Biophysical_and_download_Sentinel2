import datetime as dt
from pathlib import Path
import shapely
from sentinelhub import BBox, CRS, SHConfig
import data_loading as data

#################
# Configuration #
#################

data_path = Path("downloaded_images")
data_path.mkdir(exist_ok=True)

# Define Area of Interest (AOI)
aoi_bbox = shapely.box(736415.5330608572, 4971168.900643755, 
                        749662.1820030723, 4982112.861078258).bounds

# Define CRS for the AOI
aoi_crs = data.CRS(32632)

# Define the time range for the images
time_range = (dt.date(2020, 1, 1), dt.date(2024, 12, 30))

# Define the maximum allowed cloud coverage
max_cloud_coverage = 0.2  # 20%

# SentinelHub API Credentials (Update these)
copernicushub_config = data.create_configuration(client_id="IL_TUO_ID",  # Replace with your client ID 
    client_secret="LA_TUA_SECRET")  # Replace with your client secret

# Download images
images = data.load_satellite_images(
    config=copernicushub_config,
    aoi={'bbox': aoi_bbox, 'crs': aoi_crs},
    time_range=time_range,
    file_path=data_path,
    show_progress=True
)

print(f"✅ Download completed. {len(images)} images downloaded and saved in {data_path}")
