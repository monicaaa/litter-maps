# -*- coding: utf-8 -*-
"""Find coordinates for every street address in Philidelphia.

This file will utilize the googlemaps API to map all street addresses in
Philidelphia to its respective longitude and latitude coordinates.

How to run:
    1. Ensure that you have populated the api_key.py template with your google
       api key.
    2. Run `python3 -i coordinates_extractor.py`
    3. Verify data was written to `data/Litter_Index_Blocks_With_Coords.csv`
"""
import pandas as pd
import googlemaps

from api_key import google_api_key  # This is where the API Key lives

LITTER_INDEX_FILE = "data//Litter_Index_Blocks.csv"
FINAL_LITTER_INDEX_FILE = "data//Litter_Index_Blocks_With_Coords.csv"
GMAPS = googlemaps.Client(key=google_api_key)


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
    data = data.iloc[:25]

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


if __name__ == '__main__':
    # Main function
    data_and_geocode = extract_all_geocodes()
    data_and_geocode.to_csv(FINAL_LITTER_INDEX_FILE)
