import os
import urllib
import urllib.request
from pathlib import Path
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

# change the global display options for pandas and geopandas
# config
download_dir = Path(r"C:\Users\AndrewP\Canadian Wildlife Federation\Conservation Science General - Documents\Freshwater\Fish Passage\WCRPs\PEI\Fortune River\Model Data\elevation\raw_data")

def autorename(filename):
    """recursive function to add "_c" to a filename if it already exists.
    """
    if os.path.isfile(filename):
        newfilename = f"{os.path.splitext(filename)[0]}_c{os.path.splitext(filename)[1]}"
        return autorename(newfilename)
    else:
        return filename

path = 'https://ftp.maps.canada.ca/pub/elevation/dem_mne/highresolution_hauteresolution/dtm_mnt/1m/PEI/Prince_Edward_Island_2020/utm20/dtm_1m_utm20_e_'

# file list derived from QGIS - needed to manually select the indexes of interest
file_list = [
'3_113.tif',
'3_112.tif',
'4_113.tif',
'4_112.tif',
'5_113.tif',
]

file_list.sort()

# download all files in file list
for url in file_list:

    url = path + url

    print(url)
    # Split on the rightmost / and take everything on the right side of that
    name = url.rsplit('/', 1)[-1]

    # Combine the name and the downloads directory to get the local filename
    filename = os.path.join(download_dir, name)

    download_dir.mkdir(parents=True, exist_ok=True)

    print("Downloading", name)
    urllib.request.urlretrieve(url, autorename(filename))

print("Done!")