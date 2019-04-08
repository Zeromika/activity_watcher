import os
import sys
import logging
import subprocess, sys
import config
import requests
import json
import sqlalchemy as db
import threading

from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base


# Get id of the video to be processed.
try:
    args = sys.argv
    vid_id = args[1]
    logging.info("Parse Successfull")
except Exception as e:
    logging.error("Could not parse given data. Gracefully exiting...")
    sys.exit(500)

DB_PATH = config.DB_DETAILS['DB_PATH']

#If environment DB_PATH is set use that engine instead
if DB_PATH:
    engine = create_engine(DB_PATH, echo=True)
else:
    engine = create_engine('sqlite:///anomaly.db', echo=True)
Base = declarative_base()

Base.metadata.create_all(engine)
connection = engine.connect()
metadata = db.MetaData()
anomalies = db.Table('DetectedAnomalies', metadata, autoload=True, autoload_with=engine)
#Check with sql statement and get the whole stream of video.
video_table = db.Table('Videos',metadata,autoload=True, autoload_with=engine)

video_details = connection.execute(db.select([video_table]).where(video_table._columns.VideoId == vid_id)).fetchall()[0]
vid_name = video_details['Name']
# ffmpeg -i input -vf scale=iw/2:-1 output

def exec_long_running_proc(command, args):
    cmd = "{} {}".format(command, " ".join(str(arg) if ' ' not in arg else arg.replace(' ','\ ') for arg in args))
    print(cmd)
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # Poll process for new output until finished
    while True:
        nextline = process.stdout.readline().decode('UTF-8')
        if nextline == ' JSON-stream sent.':
            thread1 = threading.Thread(target = grabResults, args = (db, connection))
            thread1.start()
            print("List of threads: ", threading.enumerate())
        if nextline == '' and process.poll() is not None:
            break
        sys.stdout.write(nextline)
        sys.stdout.flush()

    output = process.communicate()[0]
    exitCode = process.returncode

    if (exitCode == 0 and threading.active_count==0):
        return output
    else:
        raise Exception(command, exitCode, output)


def grabResults(db,connection):
    r = requests.get('http://34.73.60.123:4010/')
    processed_results = r.json()

    for result in processed_results:
        if len(result['objects']):
            for obj in result['objects']:
                query = connection.execute(db.insert(anomalies).values(detected_anomaly = "Running Person", frame_id=result['frame_id'], center_x = obj['relative_coordinates']['center_x'], center_y = obj['relative_coordinates']['center_y'], width = obj['relative_coordinates']['width'], height = obj['relative_coordinates']['height']))

    print("Finished Processing " + vid_name)

exec_long_running_proc("../darknet./darknet", args=["detector", "demo", "./data/obj.data", "./cfg/yolo-activity-detect.cfg", "./yolo-activity.weights", "to_be_processed/vid.mkv", "-json_port", "4050", "-dont_show", "-ext_output"])

#./darknet detector demo ./data/obj.data ./cfg/yolo-activity-detect.cfg ./yolo-activity.weights to_be_processed/vid.mkv -json_port 4050 -dont_show -ext_output