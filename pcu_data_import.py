""" This script is for importing PCU data to Operations Master Sheet

    It takes an extracted telemetry file in .csv format, cleans the data, 
    and imports it to the master sheet.
    
    Note that this only works when a file is more than 3KB. Otherwise, a separate 
    script is used.
"""


from collections import Counter
import pandas as pd
from pytz import timezone
import pygsheets
import argparse
import pymap3d
import pytz
import math
import os
import requests
import ephem
from datetime import datetime, timedelta

# Construct argument parser
ap = argparse.ArgumentParser()

#Add the arguments to the parser
ap.add_argument("-r", "--row", required=True, help="row number Ops Master Sheet")
ap.add_argument("-f", "--file", required=True, help="Telemetry (.csv file) to process")
args = vars(ap.parse_args())
n_1 = int(args['row'])
n_2 = args['file']


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
def clean_data(file):
    local = timezone('Asia/Singapore')

    file_stat = os.stat(file)
    if float(file_stat.st_size) > float(3000.0):
        try:
            df = pd.read_csv(file, sep=',', header=[0, 1, 2])
            df.columns = df.columns.map('_'.join)
            df.rename_axis('SCU').reset_index()

            if df.shape[0] > 2:

                # storing datetime values to lists
                # col1 = df["SCU_SCU-TI_sec"].tolist()
                col2 = df["SCU_SCU-UTC_UTC"].tolist()
                col3 = df["SCU_PC-JST_JST"].tolist()
                col4 = df["GPS_UTC_V"].tolist()

                # Davao GS LLA
                # glat = 7.1369766
                # glon = 125.649369
                # galt = 37.0

                # ASTI GS LLA
                glat = 14.647
                glon = 121.072
                galt = 63.0

                # declaring new lists for storing parsed date values from previous lists
                # new_col1 = []
                new_col2 = []
                new_col3 = []
                new_col4 = []
                res_list1a = []
                res_list2a = []
                res_list3a = []
                res_list4a = []
                x_ecef = []
                y_ecef = []
                z_ecef = []
                az = []
                el = []
                rng = []
                t_elapsed = []
                t_ind = []

                # storing date values to new column 2
                for idx, i in enumerate(col2):
                    a = i.split(' ')
                    new_col2.append(a[0])
                    # checking time value for error and storing index if found invalid
                    try:
                        t = datetime.strptime(a[1], '%H:%M:%S')
                    except:
                        res_list2a.append(idx)

                # storing indices of values not equal to the value with max number of occurrences
                # value with max no. of occurrences is the correct value, values not equal to this is considered as an outlier
                counter2 = Counter(new_col2)
                error_val = max(counter2, key=counter2.get)
                res_list2b = [i for i, value in enumerate(new_col2) if value != error_val]
                res_list2 = res_list2a + res_list2b

                # storing date values to new column 3
                for idx, i in enumerate(col3):
                    a = i.split(' ')
                    new_col3.append(a[0])
                    # checking time value for error and storing index if found invalid
                    try:
                        t = datetime.strptime(a[1], '%H:%M:%S')
                    except:
                        res_list3a.append(idx)

                # storing indices of values not equal to the value with max number of occurrences
                # value with max no. of occurrences is the correct value, values not equal to this is considered as an outlier
                counter3 = Counter(new_col3)
                error_val = max(counter3, key=counter3.get)
                res_list3b = [i for i, value in enumerate(new_col3) if value != error_val]
                res_list3 = res_list3a + res_list3b

                # storing date values to new column 4
                for idx, i in enumerate(col4):
                    a = i.split(' ')
                    new_col4.append(a[0])
                    # checking time value for error and storing index if found invalid
                    try:
                        t = datetime.strptime(a[1], '%H:%M:%S')
                    except:
                        res_list4a.append(idx)

                # storing indices of values not equal to the value with max number of occurrences
                # value with max no. of occurrences is the correct value, values not equal to this is considered as an outlier
                counter4 = Counter(new_col4)
                error_val = max(counter4, key=counter4.get)
                res_list4b = [i for i, value in enumerate(new_col4) if value != error_val]
                res_list4 = res_list4a + res_list4b

                # merging all res_lists to produce a single error list
                # error list contains all indices with erroneous data
                error_list = (res_list2 + res_list3 + res_list4)
                # removing duplicate indices
                error_list = list(set(error_list))
                error_list.sort()

                data = df.drop(labels=error_list, axis=0)

                if data.shape[0] > 2:

                    # finding disconnections during pass
                    t_elapsed = data["QL_Elapsed Time_sec"].tolist()

                    t = 0
                    i = 0
                    for i in range(len(t_elapsed)):
                        t = t_elapsed[i] - t_elapsed[i - 1]
                        if t > 5.0:
                            t_ind.append(i)
                        i = i + 1

                    # converting ECEF coordinates to AER
                    x_ecef = data["GPS_ECF-X_V"].tolist()
                    y_ecef = data['GPS_ECF-Y_V'].tolist()
                    z_ecef = data['GPS_ECF-Z_V'].tolist()

                    # AER at start
                    xs_ecef = x_ecef[0] * 1000
                    ys_ecef = y_ecef[0] * 1000
                    zs_ecef = z_ecef[0] * 1000
                    # print(xs_ecef, ys_ecef, zs_ecef)
                    azs, els, rngs = pymap3d.ecef2aer(xs_ecef, ys_ecef, zs_ecef, glat, glon, galt, ell=None, deg=True)

                    # AER at end
                    xe_ecef = x_ecef[-1] * 1000
                    ye_ecef = y_ecef[-1] * 1000
                    ze_ecef = z_ecef[-1] * 1000
                    aze, ele, rnge = pymap3d.ecef2aer(xe_ecef, ye_ecef, ze_ecef, glat, glon, galt, ell=None, deg=True)
                    # print(pymap3d.ecef2geodetic(xe_ecef, ye_ecef, ze_ecef, ell=None, deg=True))

                    # start time
                    t_contact = data["SCU_SCU-UTC_UTC"].tolist()
                    t_start = t_contact[0].split(' .')[0]
                    tf_start = datetime.strptime(t_start, "%Y/%m/%d %H:%M:%S")
                    tf_start = tf_start.replace(tzinfo=pytz.UTC)
                    tf_start = tf_start.astimezone(local)

                    # end time
                    t_end = t_contact[-1].split(' .')[0]
                    tf_end = datetime.strptime(t_end, "%Y/%m/%d %H:%M:%S")
                    tf_end = tf_end.replace(tzinfo=pytz.UTC)
                    tf_end = tf_end.astimezone(local)

                    t_access = tf_end - tf_start

                    t_access = str(t_access)[-5:]

                    t_start = datetime.strptime(t_start, "%Y/%m/%d %H:%M:%S")
                    t_end = datetime.strptime(t_end, "%Y/%m/%d %H:%M:%S")
                    #Get Data in space track

                    space_track_username = "angulovench@gmail.com"
                    space_track_password = "Pa55w0rd!123456"

                    # Satellite's NORAD Catalog ID (replace with the NORAD ID of the satellite you're interested in)
                    norad_cat_id = "43678"  # Example: International Space Station (ISS)

                    # Ground station coordinates (replace with your ground station's coordinates)
                    ground_station_coords = {
                        'latitude': 14.6472,  
                        'longitude': 121.072027,  
                        'altitude': 78,  
                    }

                    # Time of observation in UTC (replace with your desired time)
                    observation_time_utc = t_start
                    observation_time_utc_end = t_end

                    # Fetch historical TLE data from space-track.org

                    start_date = observation_time_utc.date() - timedelta(days=1)
                    end_date = observation_time_utc.date()

                    # start_date = "2023-07-31"
                    # end_date = "2023-08-01"
                    tle_data = fetch_historical_tle(space_track_username, space_track_password, norad_cat_id,start_date, end_date)

                   # Use the latest TLE data for the specified observation time
                    satellite_tle = tle_data[-2:]  # The last two lines contain the most recent TLE data
                    azimuth, elevation = calculate_azimuth_elevation(satellite_tle, ground_station_coords,observation_time_utc)
                    azimuth_end, elevation_end = calculate_azimuth_elevation(satellite_tle, ground_station_coords,observation_time_utc_end)


                    # Create the Client
                    client = pygsheets.authorize(service_account_file="ops-master-sheet-importer-df82b6618160.json")

                    # opens a spreadsheet by its name/title
                    spreadsht = client.open("Diwata-2 Operations Master Sheet 2.0 [PhilSA]")

                    # opens a worksheet by its name/title
                    worksht = spreadsht.worksheet("title", "Pass Designation")

                    val = [tf_start.strftime("%Y/%m/%d %H:%M:%S"), azs, azimuth, els, elevation,tf_end.strftime("%Y/%m/%d %H:%M:%S"), aze, azimuth_end, ele, elevation_end, t_access]

                    # insert values in specified range
                    worksht.update_row(n_1, val, col_offset=29)

                    print('Start Time: ', observation_time_utc)
                    print(f"Azimuth: {azs} degrees")
                    print(f"Azimuth(sgp4): {azimuth} degrees")

                    print(f"Elevation: {els} degrees")
                    print(f"Elevation(sgp4): {elevation} degrees")

                    print('\n')
                    print('End Time: ', observation_time_utc_end)
                    print(f"Azimuth: {aze} degrees")
                    print(f"Azimuth(sgp4): {azimuth_end} degrees")

                    print(f"Elevation: {ele} degrees")
                    print(f"Elevation(sgp4): {elevation_end} degrees")

                else:
                    err = data.shape[0]
                    if err == 0:
                        print("After data-cleaning: No data available")
                    elif err == 1:
                        print("After data-cleaning: Only one row of data is found.")
            else:
                err = df.shape[0]
                if err == 0:
                    print("No data available")
                elif err == 1:
                    print("Only one row of data is found.")

        except Exception as e:
            print(e)
    else:
        print("File size is less than 3KB")




if __name__ == '__main__':
    try:
        file_path = n_2  
        with open(file_path, "r") as file:
            clean_data(n_2)
    except FileNotFoundError:
        print("File not found")




