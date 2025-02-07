# Sentinel-2 Image Downloader

This repository contains scripts to download Sentinel-2 satellite images using the **Sentinel Hub API**. The scripts allow querying, processing, and downloading images within a specified area and timeframe.

## Features
- Query Sentinel-2 satellite images using **Copernicus Hub API**
- Download images within a **specified bounding box (AOI) and time range**
- Supports cloud filtering with a maximum cloud coverage threshold
- Efficient processing and storage of images

## Repository Structure
```
📁 project-folder
│── 📜 data_loading.py      # Handles API requests, querying, and downloading
│── 📜 downloadS2.py        # Main script to configure and trigger downloads
│── 📜 requirements.txt     # Dependencies
│── 📁 downloaded_images/   # Folder where images are stored
```

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

## Usage

Run the script to start downloading Sentinel-2 images:

```sh
python downloadS2.py
```

The script will:
- Connect to the **Sentinel Hub API**.
- Query for images in the defined **Area of Interest (AOI)**.
- Download and save the images inside the `downloaded_images/` folder.

## Customize Your Area of Interest (AOI)

Modify the bounding box coordinates in `downloadS2.py`:
```python
aoi_bbox = shapely.box(
    MIN_X, MIN_Y,  # Lower-left corner (longitude, latitude)
    MAX_X, MAX_Y   # Upper-right corner (longitude, latitude)
).bounds
```
- Find coordinates using [geojson.io](https://geojson.io/) or another GIS tool.
- Ensure the **CRS (Coordinate Reference System)** matches Sentinel-2 (e.g., `CRS(32632)` for UTM zone 32N).

## Example Output
```
✅ Download completed. 10 images downloaded and saved in downloaded_images/
```

## License
This project is **open-source** under the MIT License.
