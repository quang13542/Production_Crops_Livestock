from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time

# Set the download directory path
import os
download_dir = os.getcwd()

# Set Chrome options
chrome_options = Options()
# chrome_options.add_argument("--headless")  # Run in headless mode (optional)
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_experimental_option('prefs', {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

# Instantiate the Chrome WebDriver with the specified options
driver = webdriver.Chrome(options=chrome_options)

faostat_url = 'https://fenixservices.fao.org/faostat/static/bulkdownloads/'

dataset_name = [
    'ConsumerPriceIndices_E_All_data',
    'Deflators_E_All_Data',
    'Prices_E_All_Data',
    'Production_Crops_Livestock_E_All_Data',
    'Trade_CropsLivestock_E_All_Data',
    'Trade_DetailedTradeMatrix_E_All_Data'
]
for name in dataset_name:
    # Navigate to the download URL
    driver.get(faostat_url+name+'.zip')

    # Wait for the file to be downloaded
    file_path = os.path.join(download_dir, name+".zip")
    while not os.path.exists(file_path):
        time.sleep(1)

# Close the WebDriver
driver.quit()
