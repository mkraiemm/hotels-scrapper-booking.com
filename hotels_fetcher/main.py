import csv
import os
import asyncio
import logging
import aiohttp
from datetime import datetime
from review_fetcher import fetch_and_filter_reviews

# Extract hotel IDs from a text file
def extract_hotel_ids(file_path):
    with open(file_path, 'r') as file:
        hotel_ids = [line.strip() for line in file.readlines()]
    return hotel_ids

# Process a single hotel: fetch reviews and details asynchronously
async def process_hotel(hotel_id, csv_writer, line_number, session):
    print(f"Starting processing hotel ID {hotel_id} (line {line_number})")
    filtered_reviews, hotel_details = await fetch_and_filter_reviews(hotel_id, session)

    if filtered_reviews and hotel_details:
        csv_writer.writerow([
            hotel_details['hotel_id'],
            "",
            hotel_details['hotel_name'],
            hotel_details['address'],
            "",
            "",
            f"URL: {hotel_details['url']}",
            "",
            ""
        ])
        csv_writer.writerows(filtered_reviews)

    print(f"Completed processing hotel ID {hotel_id} (line {line_number})")

# Main function to orchestrate the fetching process
async def main():
    start_time = datetime.now()  # Start time for timing

    hotel_ids = extract_hotel_ids("hotels_fetcher/input/hotel_ids.txt")
    csv_filename = "hotels_fetcher/output/singapore_bedbugs_reviews.csv"
    csv_headers = ["Hotel ID", "Review ID", "Hotel Name", "Address", "Review Date", "Rating", "Review Text", "Reviewer Name", "Hotel Response"]

    with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        if os.stat(csv_filename).st_size == 0:  # Write headers if the file is empty
            csv_writer.writerow(csv_headers)

        # Set up a connector with increased limits
        connector = aiohttp.TCPConnector(limit_per_host=20, limit=100)

        # Use the configured connector in the session
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [process_hotel(hotel_id, csv_writer, line_number, session) for line_number, hotel_id in enumerate(hotel_ids, start=1)]
            await asyncio.gather(*tasks)

    # Calculate and log the time taken
    end_time = datetime.now()
    time_taken = (end_time - start_time).total_seconds() / 60  # Time in minutes
    logging.info(f"Execution finished in {time_taken:.2f} minutes.")

    print(f"\nAll filtered reviews have been saved to '{csv_filename}'.")
    print(f"Execution finished in {time_taken:.2f} minutes.")

if __name__ == "__main__":
    logging.basicConfig(filename='hotels_fetcher/execution_time.log', level=logging.INFO, format='%(asctime)s - %(message)s')
    asyncio.run(main())
