import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import zipfile
# Set the download directory path
import os

download_dir = os.path.dirname(os.path.realpath(__file__))

DATASET_NAME = [
    'ConsumerPriceIndices_E_All_data',
    'Deflators_E_All_Data',
    'Prices_E_All_Data',
    'Production_Crops_Livestock_E_All_Data',
    'Trade_CropsLivestock_E_All_Data',
    'Trade_DetailedTradeMatrix_E_All_Data'
]


def get_zip():
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

    for name in DATASET_NAME:
        # Navigate to the download URL
        driver.get(faostat_url+name+'.zip')

        # Wait for the file to be downloaded
        file_path = os.path.join(download_dir, name+".zip")
        while not os.path.exists(file_path):
            time.sleep(1)

    # Close the WebDriver
    driver.quit()


def unzip_dataset():
    for name in DATASET_NAME:
        # folder_name = os.path.join(download_dir, 'dataset')
        folder_name = os.path.join(download_dir, name)
        for file in os.listdir(folder_name):
            file_path = os.path.join(folder_name, file)
            print(file_path)
            if os.path.isfile(file_path):
                os.remove(file_path)
        with zipfile.ZipFile(f'{folder_name}.zip', 'r') as zip:
            zip.extractall(path=folder_name)
        os.remove(f'{folder_name}.zip')


# if __name__ == '__main__':
    # get_zip()
    # unzip_dataset()
