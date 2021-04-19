# -*- coding: utf-8 -*-
"""Extract image for every street address in Philidelphia.

This file will utilize the googlemaps API to map all street addresses in
Philidelphia to its respective longitude and latitude coordinates. This mapping
will then be used to extract images for each longitude and latitude coordinate.

How to run:
    1. Ensure that you have populated the api_key.py template with your google
       api key.
    2. Run `python3 -i coordinates_extractor.py`
    3. Verify data was written to `data/Litter_Index_Blocks_With_Coords.csv`
    4. Verify 'downloads' folder is poplulated with a bunch of images.
"""
from threading import Thread
import time
import os

import pandas as pd
import googlemaps
import google_streetview.api

from api_key import google_api_key  # This is where the API Key lives

LITTER_INDEX_FILE = "data//Litter_Index_Blocks.csv"
FINAL_LITTER_INDEX_FILE = "data//Litter_Index_Blocks_With_Coords.csv"
GMAPS = googlemaps.Client(key=google_api_key)

# Threading is utilized to speed up the extraction of images.
# Threading is the best choice here because there is network latency when
# extracting the images through the google api.
N_JOBS = os.cpu_count() * 2  # Number of Threads to run
THREAD_QUEUE = []  # This will be our queue to manage our threads


def extract_geocode(address):
    """Search for geocode of given address.

    This function will return the geocode of the given address. A geocode has
    a lot of information attatched to it including longitude and latitude
    coordinates. We will worry about cleaning up the geocode later.

    Parameters
    ----------
    address : String
        Address to search for geocode
    """
    print(address)
    geocode_result = GMAPS.geocode(address)
    return geocode_result[0]


def extract_lat(geocode):
    """Extract latitude from geocode result.

    Extracts latitude from the geocode result (given by the google api).

    Parameters
    ----------
    geocode: dict
        Dictionary of geocode results
    """
    return geocode['geometry']['location']['lat']


def extract_lng(geocode):
    """Extract longitude from geocode result.

    Extracts longitude from the geocode result (given by the google api).

    Parameters
    ----------
    geocode: dict
        Dictionary of geocode results
    """
    return geocode['geometry']['location']['lng']


def extract_all_geocodes():
    """Extract longitude and latitude coordinates for street locations.

    This method will extract all coordinates for every street address in our
    CSV file containing our Litter Indexes.
    """
    # TODO: Drop duplicate addresses. Before that is done, we must understand
    # why duplicates are happening in the first place.
    # Read CSV
    data = pd.read_csv(LITTER_INDEX_FILE)

    # Truncating data for now because of cost... Serves as testing for now
    data = data.iloc[:5]

    # Apply coordinates extractor to every row
    data['geocode_result'] = data.apply(lambda row: extract_geocode(
                    row['LR_HUNDRED_BLOCK'] + ", Philidelphia, PA"), axis=1)

    # Extract latitude
    data['lat'] = data.apply(lambda row: extract_lat(
                    row['geocode_result']), axis=1)

    # Extract longitude
    data['lng'] = data.apply(lambda row: extract_lng(
                    row['geocode_result']), axis=1)

    return data


def extract_image(lat, lng, folder_name):
    """Extract image for given latitude and longitude coordinate.

    Uses the google street view API to an extract an image from a specified
    longitude and latitude coordinates. This image is then saved to a file and
    the name of the file is mapped back to the ID.

    Parameters
    ----------
    lat : String
        Latitude of interest
    lng : String
        Longitude of interest
    folder_name : String
        Location to save our files
    """
    folder_path = f'image_downloads/{folder_name}'

    # Create headings 0-315 (increments of 45 degrees)
    # NOTE: We will decrease the increments due to cost of extracting images
    headings = [x for x in range(0, 360, 45)]

    # Define parameters for street view API
    # Will create a list of params for each heading
    params = [{
        'size': '256x256',  # Max 640x640 pixels
        'location': f'{lat},{lng}',  # Coordinates
        'heading': heading,  # Direction TODO: We may want to experiment?
        'pitch': '0',  # Orientation (up vs down)
        'key': google_api_key  # API Key
    } for heading in headings]

    # Create a results object
    results = google_streetview.api.results(params)

    # Download images to directory 'downloads'
    print(f"Saving image to {folder_path}")
    results.download_links(folder_path)

    # Save links - Won't need to use, but nice to have
    results.save_links(f'{folder_path}/links.txt')


def extract_images_worker():
    """Thread worker for extracting images.

    This function serves as a thread worker. This means that a single thread
    will be running this function. This function will check to see if the
    THREAD_QUEUE is empty or not empty. If it is not empty, the thread will
    then utilize its resource to complete this job for the particular image.

    The idea behind this stems from the Producer and Consumer problem.
    """
    while len(THREAD_QUEUE) > 0:
        kwargs = THREAD_QUEUE.pop()
        extract_image(**kwargs)


if __name__ == '__main__':
    # Main function
    start_time = time.time()
    print("Begin extracting coordinates.")
    data_and_geocode = extract_all_geocodes()
    data_and_geocode.to_csv(FINAL_LITTER_INDEX_FILE)
    print("Done extracting coordinates.")

    print("\nBegin extracting images.")
    for idx, row in data_and_geocode.iterrows():
        lat = row['lat']
        lng = row['lng']
        object_id = row['OBJECTID']
        
        # Add "jobs" to our queue. A job here is just a set of kwargs that will
        # be passed into the extract_image function
        THREAD_QUEUE.append(
            {
                'lat': lat,
                'lng': lng, 
                'folder_name': object_id
            }
        )

    # Begin threading for multiple jobs
    image_threads = []
    for i in range((N_JOBS)):
        t = Thread(target=extract_images_worker)
        t.start()
        image_threads.append(t)

    # Join threads back together
    for t in image_threads:
        t.join()
    
    print("Done extracting images.")

    # Display total execution time
    total_time = round(time.time() - start_time, 2)
    print(f"Execution time: {total_time} seconds")
