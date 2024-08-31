import requests
import csv
from datetime import datetime, timedelta
import json
import os
import re


# Replace with your RapidAPI key
RAPIDAPI_KEY = "6cab26071fmsh1750048edcac7d5p1a0d50jsn0ac61a14f09d"

# Endpoints for searching hotels by coordinates and fetching reviews
DESTINATION_SEARCH_URL = "https://booking-com15.p.rapidapi.com/api/v1/hotels/searchDestination"
SEARCH_HOTELS_URL = "https://booking-com15.p.rapidapi.com/api/v1/hotels/searchHotels"
GET_REVIEWS_URL = "https://booking-com15.p.rapidapi.com/api/v1/hotels/getHotelReviews"

# Set up headers with your RapidAPI key
headers = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "booking-com15.p.rapidapi.com"
}

# Function to fetch reviews for a specific hotel and filter by keywords
def fetch_and_filter_reviews(hotel_id):
    print(f"Fetching reviews for hotel ID: {hotel_id}")
    current_page = 1
    filtered_reviews = []
    total_reviews = 0
    current_date = datetime.now()
    three_years_ago = current_date - timedelta(days=3*365)

    # Open the specific text file for writing
    with open(f"{hotel_id}.txt", "w", encoding="utf-8") as review_file:
        while True:
            querystring = {
                "hotel_id": hotel_id,
                "sort_option_id": "sort_most_relevant",
                "page_number": str(current_page),
                "languagecode": "en-us"
            }

            response = requests.get(GET_REVIEWS_URL, headers=headers, params=querystring)

            if response.status_code == 200:
                data = response.json()
                reviews = data.get('data', {}).get('result', [])

                if not reviews:
                    break  # No more reviews

                total_reviews += len(reviews)

                for review in reviews:
                    # Check if the review is in English and within the last 3 years
                    review_date_str = review.get('date', '')
                    try:
                        review_date = datetime.strptime(review_date_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            review_date = datetime.strptime(review_date_str, '%Y-%m-%d')
                        except ValueError:
                            print(f"Unable to parse date: {review_date_str}")
                            continue

                    if review.get('languagecode', '').startswith('en') and review_date >= three_years_ago:
                        review_id = review.get('review_id', 'N/A')
                        review_text = f"Pros: {review.get('pros', 'N/A')}\nCons: {review.get('cons', 'N/A')}".lower()
                        
                        # Write review ID and content to the text file
                        review_file.write(f"Review ID: {review_id}\n")
                        review_file.write(f"Review content: {review_text}\n\n")

                        pattern = r'\bb\s*e\s*d\s*b\s*u\s*g\s*(s)?\b'
                        match = re.search(pattern, review_text)
                        if match:
                            reviewer_name = review.get('author', {}).get('name', 'N/A')
                            rating = review.get('rating', 'N/A')
                            hotel_response = review.get('hotelier_response_date', 'No response')
                            hotel_name = review.get('hotel_name', 'N/A')
                            address = review.get('hotel_address', 'N/A')

                            filtered_reviews.append([hotel_name, address, review.get('date', 'N/A'), rating, review_text, reviewer_name, hotel_response])

                current_page += 1 
            else:
                print(f"Failed to retrieve data for hotel ID {hotel_id}: {response.status_code}")
                break

    print(f"Total reviews for hotel ID {hotel_id}: {total_reviews}")
    print(f"Reviews containing bed bugs for hotel ID {hotel_id} in the last 3 years: {len(filtered_reviews)}")
    return filtered_reviews, total_reviews

# Function to extract hotel IDs from CSV
def extract_hotel_ids(csv_filename="singapore_hotels.csv"):
    hotel_ids = []
    
    with open(csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        
        for row in csv_reader:
            hotel_ids.append(row['Hotel ID'])
    
    return hotel_ids

# Main execution
hotel_ids = [1383555] # extract_hotel_ids()
print(f"Extracted {len(hotel_ids)} hotel IDs.")
print("Hotel IDs:", hotel_ids)

# Create or open the CSV file to store all filtered reviews
csv_filename = "singapore_bedbugs_reviews.csv"
csv_headers = ["Hotel Name", "Address", "Review Date", "Rating", "Review Text", "Reviewer Name", "Hotel Response"]

with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:  # Open in append mode
    csv_writer = csv.writer(csvfile)

    if os.stat(csv_filename).st_size == 0:  # Check if the file is empty to write headers
        csv_writer.writerow(csv_headers)

    total_reviews_all_hotels = 0
    print(f"Processing {len(hotel_ids)} hotels near the specified coordinates.")

    for i, hotel_id in enumerate(hotel_ids, start=1):
        print(f"\nProcessing hotel {i}/{len(hotel_ids)} (ID: {hotel_id})")

        # Write a row with just the hotel ID
        csv_writer.writerow([f"Hotel ID: {hotel_id}", "", "", "", "", "", ""])

        filtered_reviews, total_reviews = fetch_and_filter_reviews(hotel_id)
        total_reviews_all_hotels += len(filtered_reviews)

        # Write the filtered reviews immediately after fetching them
        csv_writer.writerows(filtered_reviews)

        print(f"Completed processing hotel {i}/{len(hotel_ids)} (ID: {hotel_id}).")
        print(f"Total reviews: {total_reviews}")
        print(f"Filtered reviews: {len(filtered_reviews)}")
        # Add progress percentage
        progress_percentage = (i / len(hotel_ids)) * 100
        print(f"Progress: {progress_percentage:.2f}%\n")

print(f"\nAll filtered reviews have been saved to '{csv_filename}'. Total reviews matching criteria: {total_reviews_all_hotels}")
