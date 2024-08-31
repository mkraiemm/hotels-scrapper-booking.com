import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta

# Replace with your RapidAPI key
RAPIDAPI_KEY = "b4fc1a1e9dmsh7485cc9764c4bffp17d7b2jsne09e1b644c6a"

# Endpoints for searching hotels by coordinates and fetching reviews
GET_REVIEWS_URL = "https://booking-com15.p.rapidapi.com/api/v1/hotels/getHotelReviews"
GET_HOTEL_DETAILS_URL = "https://booking-com15.p.rapidapi.com/api/v1/hotels/getHotelDetails"

# Set up headers with your RapidAPI key
headers = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "booking-com15.p.rapidapi.com"
}

# Fetch JSON data asynchronously
async def fetch_json(session, url, params):
    async with session.get(url, headers=headers, params=params) as response:
        logging.info(f"Fetching data from URL: {url} with params: {params}")
        return await response.json()

# Fetch hotel details asynchronously
async def fetch_hotel_details(session, hotel_id):
    querystring = {
        "hotel_id": hotel_id,
        "arrival_date": "2024-10-30",
        "departure_date": "2024-11-04",
        "languagecode": "en-us"
    }
    data = await fetch_json(session, GET_HOTEL_DETAILS_URL, querystring)
    data = data.get('data', {})
    if data:
        logging.info(f"Fetched hotel details for hotel ID: {hotel_id}")
        return {
            'hotel_id': hotel_id,
            'hotel_name': data.get('hotel_name', 'N/A'),
            'url': data.get('url', 'N/A'),
            'address': f"{data.get('address', 'N/A')}, {data.get('city', 'N/A')}, {data.get('country_trans', 'N/A')}"
        }
    logging.warning(f"No hotel details found for hotel ID: {hotel_id}")
    return None

# Filter for recent reviews
def is_recent_review(review_date_str, three_years_ago):
    try:
        review_date = datetime.strptime(review_date_str, '%Y-%m-%d %H:%M:%S')
        return review_date >= three_years_ago
    except ValueError:
        return False

# Check for bedbugs in review text
def contains_bedbugs(review_text):
    return "bed bugs" in review_text.lower() or "bedbugs" in review_text.lower()

# Process and filter a single review
def process_review(review, hotel_id, three_years_ago):
    review_date_str = review.get('date', 'N/A')
    if not is_recent_review(review_date_str, three_years_ago):
        return None

    review_text = f"Pros: {review.get('pros', 'N/A')}\nCons: {review.get('cons', 'N/A')}"
    if contains_bedbugs(review_text):
        return [
            hotel_id,
            review.get('review_id', 'N/A'),
            review.get('hotel_name', 'N/A'),
            review.get('hotel_address', 'N/A'),
            review_date_str,
            review.get('rating', 'N/A'),
            review_text,
            review.get('author', {}).get('name', 'N/A'),
            review.get('hotelier_response_date', 'No response')
        ]
    return None

# Fetch reviews for a specific language
async def fetch_reviews_for_language(session, hotel_id, language_code, three_years_ago):
    filtered_reviews = []
    total_reviews_fetched = 0
    current_page = 1

    while True:
        querystring = {
            "hotel_id": hotel_id,
            "sort_option_id": "sort_most_relevant",
            "page_number": str(current_page),
            "languagecode": language_code
        }
        data = await fetch_json(session, GET_REVIEWS_URL, querystring)
        reviews = data.get('data', {}).get('result', [])
        if not reviews:
            logging.info(f"No more reviews found for hotel ID: {hotel_id} after page {current_page} in language {language_code}")
            break

        total_reviews_fetched += len(reviews)
        logging.info(f"Fetched {len(reviews)} reviews for hotel ID: {hotel_id} in language {language_code}")

        for review in reviews:
            processed_review = process_review(review, hotel_id, three_years_ago)
            if processed_review:
                filtered_reviews.append(processed_review)

        current_page += 1

    logging.info(f"Total reviews fetched for hotel ID: {hotel_id} in language {language_code}: {total_reviews_fetched}")
    logging.info(f"Total filtered reviews for hotel ID: {hotel_id} in language {language_code}: {len(filtered_reviews)}")
    return filtered_reviews

# Main function to fetch and filter reviews for a hotel
async def fetch_and_filter_reviews(hotel_id, session):
    logging.info(f"Starting to fetch reviews for hotel ID: {hotel_id}")
    hotel_details = await fetch_hotel_details(session, hotel_id)
    filtered_reviews = []
    three_years_ago = datetime.now() - timedelta(days=3*365)

    # Fetch reviews for both English (UK) and English (US)
    languages = ["en-gb", "en-us"]
    for language_code in languages:
        reviews = await fetch_reviews_for_language(session, hotel_id, language_code, three_years_ago)
        filtered_reviews.extend(reviews)

    logging.info(f"Completed fetching reviews for hotel ID: {hotel_id}. Total reviews fetched: {len(filtered_reviews)}")
    return filtered_reviews, hotel_details
