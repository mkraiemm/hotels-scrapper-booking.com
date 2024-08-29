from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO
import requests
import base64
import csv
from datetime import datetime
import geoip2.database
import cloudscraper

app = FastAPI()

class Metadata(BaseModel):
    title: str
    description: str
    image: str  # Base64 encoded string

def get_session():
    scraper = cloudscraper.create_scraper(browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    })
    scraper.headers.update({
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.google.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })
    return scraper

@app.get("/scrape", response_model=Metadata)
async def scrape_url(url: str, request: Request):
    status_code = 500  # Default status code
    session = get_session()
    
    try:
        # Fetch the webpage content using cloudscraper
        response = session.get(url, timeout=30)  # Increased timeout
        response.raise_for_status()
        status_code = response.status_code

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract title, description, and image
        title = soup.find('title').text if soup.find('title') else 'No title found'
        description = soup.find('meta', attrs={'name': 'description'}) or \
                      soup.find('meta', attrs={'property': 'og:description'})
        description = description['content'] if description else 'No description found'
        image = soup.find('meta', attrs={'property': 'og:image'})
        image_url = image['content'] if image else None

        # Fetch and encode image if exists
        if image_url:
            try:
                img_response = session.get(image_url, timeout=10)
                img_response.raise_for_status()
                img = Image.open(BytesIO(img_response.content))
                buffered = BytesIO()
                img.save(buffered, format="JPEG")
                img_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')
            except Exception as e:
                img_base64 = "Error loading image"
        else:
            img_base64 = "No image found"

        result = Metadata(
            title=title,
            description=description,
            image=img_base64
        )

        return result

    except cloudscraper.exceptions.CloudflareChallengeError as e:
        status_code = 403
        error_detail = "Cloudflare challenge failed"
        raise HTTPException(status_code=status_code, detail=error_detail)
    except requests.exceptions.RequestException as e:
        status_code = e.response.status_code if hasattr(e, 'response') else 400
        error_detail = f"Request failed: {str(e)}"
        raise HTTPException(status_code=status_code, detail=error_detail)
    except Exception as e:
        status_code = getattr(e, 'status_code', 400)
        error_detail = f"Unexpected error: {str(e)}"
        raise HTTPException(status_code=status_code, detail=error_detail)

    finally:
        # Log the request with status code, regardless of success or failure
        log_request(url, request, status_code)

def log_request(url: str, request: Request, status_code: int):
    timestamp = datetime.now().isoformat()
    ip_address = request.client.host
    country = get_country(ip_address)

    with open('request_log.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([timestamp, url, ip_address, country, status_code])

def get_country(ip_address: str) -> str:
    try:
        with geoip2.database.Reader('GeoLite2-Country.mmdb') as reader:
            response = reader.country(ip_address)
            return response.country.name
    except:
        return "Unknown"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
