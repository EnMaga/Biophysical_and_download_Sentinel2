````markdown
## Sentinel-2 Image Downloader

This repository contains scripts to download Sentinel-2 satellite images using the **Sentinel Hub API** and compute biophysical variables (LAI, chlorophyll, and water content) using SNAP models.

### Features
- Query Sentinel-2 satellite images using **Copernicus Hub API** or **AWS** endpoints
- Download images within a **specified bounding box (AOI) and time range**
- Supports cloud filtering with a maximum cloud coverage threshold
- Efficient processing and storage of images
- Compute biophysical variables:
  - Leaf Area Index (LAI)
  - Chlorophyll content (Cab)
  - Water content (Cw)

### Repository Structure
```plain
📁 project-folder
│── 📜 data_loading.py           # Handles API requests, querying, and downloading
│── 📜 downloadS2.py             # Main download script
│── 📜 LAI_CCC_CWC_computation.py# Compute LAI, Cab, Cw using SNAP models
│── 📜 requirements.txt          # Dependencies
│── 📁 downloaded_images/        # Folder where downloaded Sentinel-2 images are stored
│── 📁 output/                   # Folder where biophysical outputs are saved
````

---

## Installation

Ensure you have **Python 3.8+** installed, then follow these steps:

**Clone the Repository**

```sh
git clone https://github.com/YOUR_GITHUB_USERNAME/Sentinel2-Downloader.git
cd Sentinel2-Downloader
```

**Create a Virtual Environment (Optional but Recommended)**

```sh
python -m venv venv
source venv/bin/activate  # On macOS/Linux
venv\Scripts\activate     # On Windows
```

**Install Dependencies**

```sh
pip install -r requirements.txt
```

---

## Setup Sentinel Hub API Credentials

1. Create an account on **[Sentinel Hub](https://www.sentinel-hub.com/)**.
2. Obtain your **Client ID** and **Client Secret** from the Sentinel Hub dashboard.
3. Open `downloadS2.py` and update the following section:

```python
copernicushub_config = data.create_configuration(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET"
)
```

---

## Downloading Sentinel-2 Images

Run the script to start downloading Sentinel-2 images:

```sh
python downloadS2.py
```

The script will:

* Connect to the **Sentinel Hub API** or AWS S3 endpoints.
* Query for images in the defined **Area of Interest (AOI)**.
* Download and save the images inside the `downloaded_images/` folder.

---

## Compute LAI, Chlorophyll, and Water Content

The `LAI_CCC_CWC_computation.py` script processes Sentinel-2 data to compute biophysical variables (LAI, Cab, Cw) for your study area.

### Usage

```sh
python LAI_CCC_CWC_computation.py \
  --geojson path/to/your_aoi.geojson \
  --start_date 2019-01-01 \
  --end_date   2019-02-01 \
  --out_dir    path/to/output_folder \
  --target_crs EPSG:32632
```

### Script Description

* **Input AOI**: Provide a GeoJSON file defining your area of interest.
* **Date Range**: Specify `--start_date` and `--end_date` for the analysis period.
* **Output Directory**: `--out_dir` will contain sub-folders for each variable (LAI, CCC, CWC) and each date.
* **CRS**: `--target_crs` defines the output coordinate reference system (default `EPSG:32632`).

Under the hood, the script:

1. Reads your AOI and reprojects to EPSG:4326.
2. Fetches Sentinel-2 multispectral (MS) and scene classification layer (SCL) data from AWS.
3. Applies a SNAP biophysical model to compute:

   * LAI
   * LAI\_Cab (chlorophyll)
   * LAI\_Cw (water content)
4. Reprojects outputs to the target CRS and applies a cloud mask based on the SCL band.
5. Writes Cloud-Optimized GeoTIFFs (COGs) into the output directory, organized by variable and date.

### Example Output

```bash
✅ Saved LAI for 20190101 to output/index=LAI/aoi=aoi_id/S2_20190101_000_aoi_id_LAI.tif
✅ Saved CCC for 20190101 to output/index=CCC/aoi=aoi_id/S2_20190101_000_aoi_id_CCC.tif
✅ Saved CWC for 20190101 to output/index=CWC/aoi=aoi_id/S2_20190101_000_aoi_id_CWC.tif
```

---

## Customize Your Area of Interest (AOI)

Modify the bounding box coordinates in `downloadS2.py`:

```python
from shapely import box

aoi_bbox = box(
    MIN_X, MIN_Y,  # Lower-left corner (lon, lat)
    MAX_X, MAX_Y   # Upper-right corner
).bounds
```

Find coordinates using [geojson.io](https://geojson.io/) or another GIS tool. Ensure the CRS matches Sentinel-2 (e.g., UTM zone).

---

## License

This project is **open-source** under the MIT License.

```
```
