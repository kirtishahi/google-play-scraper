import pandas as pd
from google_play_scraper import app


# Read the CSV file
input_file_path = 'merged_csv_20240617-1 (1).csv'  # Replace with your actual CSV file path
output_file_path = 'app_details_output.csv'  # File where the output data will be stored

# Read the CSV file into a DataFrame
df = pd.read_csv(input_file_path)

# List to store app details
app_details_list = []

# Function to fetch details using app method
def fetch_app_details(app_id):
    try:
        details = app(app_id,"en","us")  # Fetch app details
        app_details_list.append(details)  # Append the details to the list
        print(f"Fetched details for {app_id}")
    except Exception as e:
        print(f"Failed to fetch details for {app_id}: {e}")

# Iterate over the 'appId' column and fetch details
for app_id in df['appId'][:5]:
    fetch_app_details(app_id)

# Convert the list of details into a DataFrame
output_df = pd.DataFrame(app_details_list)

# Save the DataFrame to a CSV file
output_df.to_csv(output_file_path, index=False)

print(f"App details have been saved to {output_file_path}")