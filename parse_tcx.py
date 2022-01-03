"""Some functions for parsing a TCX file (specifically, a TCX file
downloaded from Strava, which was generated based on data recorded by a
Garmin vívoactive 3) and creating a Pandas DataFrame with the data.
"""

from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Union, Tuple
import Code.logger.logger
import lxml.etree
import pandas as pd
import dateutil.parser as dp
import pytz, tzlocal
import os, re, shutil

NAMESPACES = {
    'ns': 'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2',
    'ns2': 'http://www.garmin.com/xmlschemas/UserProfile/v2',
    'ns3': 'http://www.garmin.com/xmlschemas/ActivityExtension/v2',
    'ns4': 'http://www.garmin.com/xmlschemas/ProfileExtension/v1',
    'ns5': 'http://www.garmin.com/xmlschemas/ActivityGoals/v1'
}

# The names of the columns we will use in our laps DataFrame
LAPS_COLUMN_NAMES = ['number', 'start_time', 'distance', 'calories', 'total_time', 'max_speed', 'max_hr', 'avg_hr']

# The names of the columns we will use in our trackpoint DataFrame
TRACKPOINT_COLUMN_NAMES = ['time', 'distance', 'heart_rate', 'watt', 'cadence']

def convert_local(str_datetime):
    dt_aware = pd.to_datetime(str_datetime)
    local_timezone = tzlocal.get_localzone()
    dt_local = dt_aware.astimezone(local_timezone).replace(tzinfo=None)
    return dt_local

def get_tcx_lap_data(lap: lxml.etree._Element) -> Dict[str, Union[float, datetime, int]]:
    """Extract some data from an XML element representing a lap and
    return it as a dict.
    """
    data: Dict[str, Union[float, datetime, timedelta, int]] = {}
    
    # Note that because each element's attributes and text are returned as strings, we need to convert those strings
    # to the appropriate datatype (datetime, float, int, etc).
    
    start_time_str = lap.attrib['StartTime']
    #data['start_time'] = dp.parse(start_time_str)
    data['start_time'] = convert_local(start_time_str)

    log.debug(f'LAP - Start: {dp.parse(start_time_str)}')
    distance_elem = lap.find('ns:DistanceMeters', NAMESPACES)
    if distance_elem is not None:
        data['distance'] = float(distance_elem.text)

    calorie_elem = lap.find('ns:Calories', NAMESPACES)
    if calorie_elem is not None:
        data['calories'] = int(calorie_elem.text)

    total_time_elem = lap.find('ns:TotalTimeSeconds', NAMESPACES)
    if total_time_elem is not None:
        data['total_time'] = float(total_time_elem.text)
    
    max_speed_elem = lap.find('ns:MaximumSpeed', NAMESPACES)
    if max_speed_elem is not None:
        data['max_speed'] = float(max_speed_elem.text)
    
    max_hr_elem = lap.find('ns:MaximumHeartRateBpm', NAMESPACES)
    if max_hr_elem is not None:
        data['max_hr'] = float(max_hr_elem.find('ns:Value', NAMESPACES).text)
    
    avg_hr_elem = lap.find('ns:AverageHeartRateBpm', NAMESPACES)
    if avg_hr_elem is not None:
        data['avg_hr'] = float(avg_hr_elem.find('ns:Value', NAMESPACES).text)
    
    return data

def get_tcx_trackpoint_data(trackpoint: lxml.etree._Element) -> Optional[Dict[str, Union[float, int, str, datetime]]]:
    """Extract some data from an XML element representing a track point
    and return it as a dict.
    """
    data: Dict[str, Union[float, int, str, datetime]] = {}

    time_str = trackpoint.find('ns:Time', NAMESPACES).text
    data['time'] = convert_local(time_str)

    cadence_elem = trackpoint.find('ns:Cadence', NAMESPACES)
    if cadence_elem is not None:
        data['cadence'] = float(cadence_elem.text)

    distance_elem = trackpoint.find('ns:DistanceMeters', NAMESPACES)
    if distance_elem is not None:
        data['distance'] = float(distance_elem.text)

    hr_elem = trackpoint.find('ns:HeartRateBpm', NAMESPACES)
    if hr_elem is not None:
        data['heart_rate'] = int(hr_elem.find('ns:Value', NAMESPACES).text)

    # The ".//" here basically tells lxml to search recursively down the tree for the relevant tag, rather than just the
    # immediate child elements of speed_elem. See https://lxml.de/tutorial.html#elementpath
    watt_elem = trackpoint.find('.//ns3:Watts', NAMESPACES)
    if watt_elem is not None:
        data['watt'] = float(watt_elem.text or 0)

    return data

def get_dataframes(fname: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Takes the path to a TCX file (as a string) and returns two Pandas
    DataFrames: one containing data about the laps, and one containing
    data about the individual track points.
    """
    
    tree = lxml.etree.parse(fname)
    root = tree.getroot()
    activity = root.find('ns:Activities', NAMESPACES)[0]  # Assuming we know there is only one Activity in the TCX file
                                                          # (or we are only interested in the first one)
    points_data = []
    laps_data = []
    lap_no = 1
    for lap in activity.findall('ns:Lap', NAMESPACES):
        # Get data about the lap itself
        single_lap_data = get_tcx_lap_data(lap)
        single_lap_data['number'] = lap_no
        laps_data.append(single_lap_data)
        # Get data about the track points in the lap
        log.debug(f'Inizio LAP #: {lap_no}')   
        track = lap.find('ns:Track', NAMESPACES) 
        for point in track.findall('ns:Trackpoint', NAMESPACES):
            #single_point_data = get_tcx_point_data(point)
            single_point_data = get_tcx_trackpoint_data(point)
            if single_point_data:
                single_point_data['lap'] = lap_no
                points_data.append(single_point_data)
        lap_no += 1
    
    # Create DataFrames from the data we have collected. If any information is missing from a particular lap or track
    # point, it will show up as a null value or "NaN" in the DataFrame.
    laps_df = pd.DataFrame(laps_data, columns=LAPS_COLUMN_NAMES)
    laps_df.set_index('number', inplace=True)
    points_df = pd.DataFrame(points_data, columns=TRACKPOINT_COLUMN_NAMES)

    log.debug(f'Succesfully scanned file: {fname}')

    return laps_df, points_df

    
if __name__ == '__main__':
    
    from sys import argv
    log             = Code.logger.logger.setup_applevel_logger(file_name = 'Data/Logs/app_garmin_debug.log')
    log_excel_df    = r'Data/Logs/export_dataframe_o365.xlsx'
    writer          = pd.ExcelWriter(log_excel_df)
    #fname = argv[1]  # Path to TCX file to be given as first argument to script
    fpath = r'C:/Projects/Coxswain2Fit/Data/Activity/20220103/'

    laps_df_concept2 = []
    points_df_concept2 = []
    for file in os.listdir(fpath):
        if file.startswith("concept2"):
            laps_df_concept2_temp, points_df_concept2_temp = get_dataframes(os.path.join(fpath, file))
            laps_df_concept2.append(laps_df_concept2_temp)
            points_df_concept2.append(points_df_concept2_temp)
        if file.startswith("activity"):
            laps_df_garmin, points_df_garmin = get_dataframes(os.path.join(fpath, file))

    laps_df_concept2 = pd.concat(laps_df_concept2)
    points_df_concept2 = pd.concat(points_df_concept2)
    laps_df_concept2 = laps_df_concept2.sort_values(by='start_time',ascending=True)
    points_df_concept2 = points_df_concept2.sort_values(by='time',ascending=True)
    print('First Dataframe LAPS:')
    print(laps_df_concept2)
    print('\nSecond Dataframe TRACK POINTS:')
    print(points_df_concept2)
    print('Struttura del Dataframe LAPS:')
    print(laps_df_concept2.info())
    print('Struttura del Dataframe TRACK POINTS:')
    print(points_df_concept2.info())
    
    garmin_start        = laps_df_garmin['start_time'].min()
    garmin_track_start  = points_df_garmin['time'].min()
    garmin_track_end    = points_df_garmin['time'].max()

    concept2_start        = laps_df_concept2['start_time'].min()
    concept2_track_start  = points_df_concept2['time'].min()
    concept2_track_end    = points_df_concept2['time'].max()

    difference = (garmin_start - concept2_start)
    total_seconds = difference.total_seconds()

    log.debug(f'Garmin - Start: {garmin_start} - Track start: {garmin_track_start} end: {garmin_track_end}')
    log.debug(f'Concept2 - Start: {concept2_start} - Track start: {concept2_track_start} end: {concept2_track_end}')
    log.debug(f'Garmin vs Concept2 - Difference in sec.: {total_seconds}')   

    laps_df_garmin.to_excel(writer, 'laps_df_garmin', index = False)
    points_df_garmin.to_excel(writer, 'points_df_garmin', index = False)

    laps_df_concept2['start_time'] = laps_df_concept2['start_time'] + timedelta(seconds=total_seconds)
    laps_df_concept2.to_excel(writer, 'laps_df_concept2', index = False)
    points_df_concept2['time'] = points_df_concept2['time'] + timedelta(seconds=total_seconds)
    points_df_concept2.to_excel(writer, 'points_df_concept2', index = False)


    merged_dataframe = pd.merge_asof(points_df_concept2[['time', 'distance', 'watt', 'cadence']], points_df_garmin[['time', 'heart_rate']], on="time", by="time")
    merged_dataframe['duration'] = (merged_dataframe['time'] - merged_dataframe['time'].iloc[0]).dt.seconds
    merged_dataframe['hhmmss_1'] = pd.to_datetime(merged_dataframe["duration"], unit='s').dt.time
    #merged_dataframe['hhmmss_2'] = pd.to_datetime(merged_dataframe['duration'], format='%H:%M:%S').dt.time
    merged_dataframe.to_excel(writer, 'merged_dataframe', index = False)

    writer.save()