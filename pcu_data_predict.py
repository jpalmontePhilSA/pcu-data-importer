import requests
import ephem
import argparse
import math
import pytz
import pygsheets
from datetime import datetime, timedelta, timezone


ap = argparse.ArgumentParser()
#Add the arguments to the parser
ap.add_argument("-r", "--row", required=True, help="row number Ops Master Sheet")
ap.add_argument("-s", "--start", required=True, help="Start Time")
ap.add_argument("-e", "--end", required=True, help="End Time")
args = vars(ap.parse_args())
n_1 = int(args['row'])
n_2 = args['start']
n_3 = args['end']


def validate_datetime(input_str, format_str):
    try:
        datetime_obj = datetime.strptime(input_str, format_str)
        return datetime_obj, None  # Return None for error if no exception is raised
    except ValueError as e:
        return None, str(e)

def fetch_historical_tle(space_track_username, space_track_password, norad_cat_id, start_date, end_date):
    # Space-track.org API endpoint for historical TLE data
    url = "https://www.space-track.org/ajaxauth/login"

    # Space-track.org account credentials
    payload = {
        "identity": space_track_username,
        "password": space_track_password,
    }

    # Login to space-track.org to get a session cookie
    session = requests.Session()
    response = session.post(url, data=payload)
    response.raise_for_status()

    # Space-track.org API endpoint to retrieve TLE data
    #tle_url = f"https://www.space-track.org/basicspacedata/query/class/tle_latest/NORAD_CAT_ID/{norad_cat_id}/EPOCH/{start_date}--{end_date}/format/tle"
    tle_url = f"https://www.space-track.org/basicspacedata/query/class/gp_history/NORAD_CAT_ID/{norad_cat_id}/orderby/TLE_LINE1%20ASC/EPOCH/{start_date}--{end_date}/format/tle"
    # Fetch the historical TLE data
    response = session.get(tle_url)
    response.raise_for_status()
    # Split the response into lines to extract the TLE data
    tle_data = response.text.strip().split('\n')
    return tle_data

def calculate_azimuth_elevation(satellite_tle, ground_station_coords, time_utc):
    # Create ephem.Observer objects for satellite and ground station
    sat = ephem.readtle('Satellite', satellite_tle[0], satellite_tle[1])
    gs = ephem.Observer()
    gs.lat = math.radians(ground_station_coords['latitude'])
    gs.lon = math.radians(ground_station_coords['longitude'])
    gs.elevation = ground_station_coords['altitude']

    # Setting the time for the observation
    gs.date = time_utc

    # Compute the satellite's position at the given time
    sat.compute(gs)

    # Calculate azimuth and elevation
    azimuth = math.degrees(sat.az)
    elevation = math.degrees(sat.alt)

    return azimuth, elevation



expected_format = "%Y-%m-%d %H:%M:%S"

parsed_datetime1,error1 = validate_datetime(n_2, expected_format)
parsed_datetime2,error2 = validate_datetime(n_3, expected_format)


if parsed_datetime1 and parsed_datetime2:
    timezone_local = pytz.timezone('Asia/Singapore')
    time_start = datetime.strptime(n_2, "%Y-%m-%d %H:%M:%S")
    time_start_local = timezone_local.localize(time_start)
    time_start_utc = time_start_local.astimezone(timezone.utc)
    time_end = datetime.strptime(n_3, "%Y-%m-%d %H:%M:%S")
    time_end_local = timezone_local.localize(time_end)
    time_end_utc = time_end_local.astimezone(timezone.utc)

    time_difference = time_end - time_start
    t_access= str(time_difference)[-5:]



    if time_difference >= timedelta(minutes=0) and time_difference <= timedelta(minutes=12):
        # Space-track.org account credentials
        space_track_username = "angulovench@gmail.com"
        space_track_password = "Pa55w0rd!123456"

        # Satellite's NORAD Catalog ID
        norad_cat_id = "43678"

        # ASTI Ground station coordinates
        ground_station_coords = {
            'latitude': 14.6472,
            'longitude': 121.072027,
            'altitude': 78,
        }

        # Time of observation in UTC

        # Fetch historical TLE data from space-track.org

        start_date = time_start.date() - timedelta(days=1)
        end_date = time_start.date()

        tle_data = fetch_historical_tle(space_track_username, space_track_password, norad_cat_id, start_date, end_date)

        # Use the latest TLE data for the specified observation time
        satellite_tle = tle_data[-2:]  # The last two lines contain the most recent TLE data
        azimuth, elevation = calculate_azimuth_elevation(satellite_tle, ground_station_coords, time_start_utc)
        azimuth_end, elevation_end = calculate_azimuth_elevation(satellite_tle, ground_station_coords, time_end_utc)

        # Create the Client
        client = pygsheets.authorize(service_account_file="ops-master-sheet-importer-df82b6618160.json")

        # opens a spreadsheet by its name/title
        spreadsht = client.open("Diwata-2 Operations Master Sheet 2.0 [PhilSA]")

        # opens a worksheet by its name/title
        worksht = spreadsht.worksheet("title", "Pass Designation")

        val = [time_start_local.strftime("%Y/%m/%d %H:%M:%S"),' ', azimuth,' ', elevation,time_end_local.strftime("%Y/%m/%d %H:%M:%S"),' ', azimuth_end,' ',  elevation_end, t_access ]
        # insert values in specified range
        worksht.update_row(n_1, val, col_offset=29)

        print("Total access Time: ", t_access)
        print('\n')
        print('Start time: ',time_start)
        print(f"Azimuth: {azimuth} degrees")
        print(f"Elevation: {elevation} degrees")
        print('\n')
        print('End time: ', time_end)
        print(f"Azimuth: {azimuth_end} degrees")
        print(f"Elevation: {elevation_end} degrees")

    else:
        print("Time difference is either negative or greater than 12 minutes.")

else:
    if error1:
        print("Start Time: Wrong Date Time Format YYYY-MM-DD HH:MM:SS")
    if error2:
        print("End Time: Wrong Date Time Format YYYY-MM-DD HH:MM:SS")




