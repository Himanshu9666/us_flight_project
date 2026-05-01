import os
import requests
import zipfile
from pathlib import Path

BTS_WEBSITE = "https://transtats.bts.gov/PREZIP/"

LOCAL_SAVE_FOLDER = "./downloaded_files"
Path(LOCAL_SAVE_FOLDER).mkdir(parents=True, exist_ok=True)
print(f"Save folder ready: {LOCAL_SAVE_FOLDER}")

YEARS_TO_DOWNLOAD = [2022]


def download_one_month(year, month, save_folder):
    """
    Downloads flight data for a single month from the BTS website.
    Returns the path to the saved CSV file, or None if something went wrong.
    """

    zip_filename = (
        f"On_Time_Reporting_Carrier_On_Time_Performance_"
        f"1987_present_{year}_{month}.zip"
    )

    download_url = BTS_WEBSITE + zip_filename

    zip_save_path = os.path.join(save_folder, zip_filename)
    csv_save_path = zip_save_path.replace(".zip", ".csv")

    if os.path.exists(csv_save_path):
        print(f"Already downloaded: {year}-{month:02d} (skipping)")
        return csv_save_path

    print(f"Downloading {year}-{month:02d}")
    print(f"From: {download_url}")

    try:
        response = requests.get(download_url, stream=True, timeout=120)
        response.raise_for_status()

        # Save ZIP file
        with open(zip_save_path, "wb") as zip_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                zip_file.write(chunk)

        # Extract CSV from ZIP
        with zipfile.ZipFile(zip_save_path, "r") as zip_ref:
            csv_inside = [name for name in zip_ref.namelist() if name.endswith(".csv")][0]
            zip_ref.extract(csv_inside, save_folder)

            # Rename to consistent name
            os.rename(os.path.join(save_folder, csv_inside), csv_save_path)

        # Remove ZIP
        os.remove(zip_save_path)

        # File size
        file_size_mb = os.path.getsize(csv_save_path) / (1024 ** 2)
        print(f"Done: {csv_save_path} ({file_size_mb:.1f} MB)")

        # Row count check
        with open(csv_save_path, "r", encoding="utf-8", errors="replace") as f:
            row_count = sum(1 for _ in f) - 1

        print(
            f"Row count: {row_count:,} "
            f"{' looks good' if row_count > 400000 else ' seems low — double check!'}"
        )

        return csv_save_path

    except Exception as error:
        print(f"ERROR on {year}-{month:02d}: {error}")
        print("Tip: Check your internet connection or try again.")
        return None


for year in YEARS_TO_DOWNLOAD:
    print(f"\nYear {year}")

    for month in range(1, 4):  # months 1 to 3
        csv_file = download_one_month(year, month, LOCAL_SAVE_FOLDER)