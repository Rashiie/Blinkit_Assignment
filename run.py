import pandas as pd
import requests
from datetime import datetime
import time


def fetch_products(lat, lng):
    url = "https://blinkit.com/"
    params = {
        "lat": lat,
        "lng": lng
        
    }
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/114.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, /",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://blinkit.com/",
    "Origin": "https://blinkit.com",
    "Connection": "keep-alive"
    }
    session = requests.Session()
    session.headers.update(headers)
    response = session.get(url)
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data.get("category_detail", {}).get("products", [])
    else:
        print(f" Failed:  | Status: {response.status_code}")
        return []

# Function to extract structured row from product
def extract_row(product, category, location):
    return {
        "date": datetime.today().date(),
        "l1_category": category["category_name"],
        "l1_category_id": category["category_id"],
        "l2_category": category["sub_category_name"],  
        "l2_category_id": category["sub_category_id"],
        "store_id": location["location_name"],
        "variant_id": product.get("id"),
        "variant_name": product.get("name"),
        "group_id": product.get("group_id", ""),
        "selling_price": product.get("price", {}).get("sp"),
        "mrp": product.get("price", {}).get("mrp"),
        "in_stock": product.get("available", True),
        "inventory": product.get("inventory", {}).get("quantity", ""),
        "is_sponsored": product.get("is_sponsored", False),
        "image_url": product.get("images", {}).get("main"),
        "brand_id": product.get("brand", {}).get("id"),
        "brand": product.get("brand", {}).get("name")
    }

# Main function to run scraping
def scrape_blinkit_data(category_file, location_file, output_file="/content/sample_data/blinkit_category_scraping_stream.csv"):
    categories = pd.read_csv(category_file)
    
    print(categories)
    locations = pd.read_csv(location_file)
    all_rows = []

    for _, loc in locations.iterrows():
        for _, cat in categories.iterrows():
           
            products = fetch_products(loc['latitude'], loc['longitude'])

            for product in products:
                row = extract_row(product, cat, loc)
                all_rows.append(row)

            time.sleep(1)  

    df_out = pd.DataFrame(all_rows)
    df_out.to_csv(output_file, index=False)
    print(f"Scraped data saved to: {output_file}")


scrape_blinkit_data(
    "/content/sample_data/blinkit_categories.csv",
    "/content/sample_data/blinkit_locations.csv"
)
