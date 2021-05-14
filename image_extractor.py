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
import random
from PIL import Image

import numpy as np
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
    # Add a random number between 30 and 70 to address (i.e. 100 -> 160)
    split_address = address.split(' ')
    number_to_add = random.randrange(30, 80, 10)  # Rand num between 30 and 70
    house_number = int(split_address[0]) + number_to_add  # Add num to address
    new_address = str(house_number) + " " + " ".join(split_address[1:])
    print(new_address)

    geocode_result = GMAPS.geocode(new_address)
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


def extract_all_geocodes(data):
    """Extract longitude and latitude coordinates for street locations.

    This method will extract all coordinates for every street address in our
    CSV file containing our Litter Indexes.
    """
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


def extract_image(lat, lng, folder_name, street_score):
    """Extract image for given latitude and longitude coordinate.

    Uses the google street view API to an extract an image from a specified
    longitude and latitude coordinates. This image is then saved to a file and
    the name of the file is mapped back to the ID.

    https://ai.plainenglish.io/image-processing-and-classification-with-python-
    and-keras-c368769bde26

    Parameters
    ----------
    lat : String
        Latitude of interest
    lng : String
        Longitude of interest
    folder_name : String
        Location to save our files
    street_score : float
        Score of the images
    """
    folder_path = f'image_downloads/{folder_name}'

    # Create headings 0-270 (increments of 90 degrees)
    # NOTE: We will decrease the increments due to cost of extracting images
    headings = [x for x in range(0, 360, 90)]

    # Define parameters for street view API
    # Will create a list of params for each heading
    params = [{
        'size': '640x640',  # Max 640x640 pixels
        'location': f'{lat},{lng}',  # Coordinates
        'heading': heading,  # Direction TODO: We may want to experiment?
        'pitch': '-45',  # Orientation (up vs down)
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


def get_data(street_class_names, score_colors):
    """Extract data to query on.

    Extracts data to extract geocodes and images for. Basic data cleaning and
    filtering is completed in order to grab the data points that we want.

    Parameters
    ----------
    street_class_names: List
        Defines which street class name we want to keep.
    score_colors: List
        Defines which scores colors we want to keep.
    """
    # Read CSV
    data = pd.read_csv(LITTER_INDEX_FILE)

    # Grab streetnames whose numbers are decimal
    data = data[
        data['LR_HUNDRED_BLOCK'].str.split(' ', expand=True)[0].str.isdecimal()
    ]

    # Grab street class names
    data = data[data['STREET_CLASS_NAME'].isin(street_class_names)]

    # Filter scorecolors we are interested in
    data = data[data['SCORE_COLOR'].isin(score_colors)]

    # Drop duplicate street names
    data = data.drop_duplicates('LR_HUNDRED_BLOCK')

    # Remove OBJECTIDs we already completed
    data = data[
        ~data['OBJECTID'].astype(str).isin(os.listdir('image_downloads'))]

    # Truncating data for now because of cost... Serves as testing for now
    data = data.iloc[:1]

    return data


def extract_pixel_data():
    """Extract pixel data from all images."""
    # Read in original data set
    litter_index = pd.read_csv("data/Litter_Index_Blocks.csv")

    # Iterate through all image data and extract pixels
    image_folders = os.listdir("image_downloads")
    for folder in image_folders:
        print(folder)
        # Obtain street score for set of images
        street_score = litter_index[litter_index['OBJECTID'] == int(folder)]\
            ['HUNDRED_BLOCK_SCORE'].iloc[0]

        # Convert to image to pixel value & save
        folder_path = "image_downloads/" + folder  # Path to folder
        data = []
        for image in os.listdir(folder_path):
            if image.endswith('.jpg'):
                im = Image.open(folder_path + "/" + image)  # Load image
                # Clean image
                img = im.convert('RGB')  # Ensures correct color channel
                # img_resize = img_cs.resize(640, 640)  # Ensures correct size
                img_array = np.asarray(img)  # Extracts pixels
                # Append objectID to file
                img_array = np.append(img_array, street_score)  # Append target
                data.append(img_array)

        # Save and reshape data
        data_array = np.array(data)
        data_array = data_array.reshape(4, 640 * 640 * 3 + 1)

        # Append data_array to data file
        with open('data/pixel_data', 'ab') as f:
            np.savetxt(f, data_array, delimiter=",", fmt='%f')

        # To Load run: arr = np.loadtxt('data/pixel_data', delimiter=",")


if __name__ == '__main__':
    # Main function
    # Extract data given our conversations
    data_requirements_kwargs = {
        'street_class_names': ['Local'],
        'score_colors': ['MAROON']
    }
    data = get_data(**data_requirements_kwargs)

    start_time = time.time()
    print("Begin extracting coordinates.")
    data_and_geocode = extract_all_geocodes(data)
    data_and_geocode.to_csv(FINAL_LITTER_INDEX_FILE)
    print("Done extracting coordinates.")

    print("\nBegin extracting images.")
    for idx, row in data_and_geocode.iterrows():
        lat = row['lat']
        lng = row['lng']
        object_id = row['OBJECTID']
        score = row['HUNDRED_BLOCK_SCORE']

        # Add "jobs" to our queue. A job here is just a set of kwargs that will
        # be passed into the extract_image function
        THREAD_QUEUE.append(
            {
                'lat': lat,
                'lng': lng,
                'folder_name': object_id,
                'street_score': score
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

    print("\nBegin extracting pixel data.")
    extract_pixel_data()
    print("Done extracting pixel data.")

    # Display total execution time
    total_time = round(time.time() - start_time, 2)
    print(f"Execution time: {total_time} seconds")
