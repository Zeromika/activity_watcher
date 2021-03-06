import os
import sys
import logging
import subprocess, sys
import config
import requests
import json
import sqlalchemy as db
import threading
from math import ceil
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

logging.basicConfig(filename=config.PATH['logs'],
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
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
    engine = create_engine(DB_PATH, echo=False)
else:
    engine = create_engine('sqlite:///anomaly.db', echo=False)
Base = declarative_base()

Base.metadata.create_all(engine)
connection = engine.connect()
metadata = db.MetaData()
anomalies_table = db.Table('detected_anomalies', metadata, autoload=True, autoload_with=engine)
video_anomalies_table = db.Table('video_detected_anomaly', metadata, autoload=True, autoload_with=engine)
#Check with sql statement and get the whole stream of video.
video_table = db.Table('videos',metadata,autoload=True, autoload_with=engine)

video_details = connection.execute(db.select([video_table]).where(video_table._columns.video_id == vid_id)).fetchall()[0]
vid_name = video_details['name']
vid_w = video_details['width']
vid_h = video_details['height']
relative_size = {"width": vid_w, "height": vid_h}
# ffmpeg -i input -vf scale=iw/2:-1 output

def exec_long_running_proc(command, args):
    cmd = "{} {}".format(command, " ".join(str(arg) if ' ' not in arg else arg.replace(' ','\ ') for arg in args))
    #print(cmd)
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd= config.PATH['DARKNET_PATH'])

    # Poll process for new output until finished
    thread1 = None
    while True:
        nextline = process.stdout.readline().decode('UTF-8')
        if 'JSON-stream sent.' in nextline and not thread1:
            thread1 = threading.Thread(target = grabResults, args = (db, connection, relative_size))
            thread1.start()
            info_function(0)
            logging.warning("List of threads: " + str(threading.enumerate()))
        if nextline == '' and process.poll() is not None:
            break
        logging.warning(nextline)

    output = process.communicate()[0]
    exitCode = process.returncode

    
    while (exitCode == 0 and threading.active_count==0):
        return output
    raise Exception ("Something Went Wrong...")


def info_function(value):
    sys.stdout.write(json.dumps(dict(progress=ceil(value)), indent=2))
    sys.stdout.flush()


def grabResults(db,connection, relative_size):
    r = requests.get('http://localhost:4050')
    processed_results = r.json()
    logging.warning("Started grabbing results")
    info_function(len(processed_results)/100)
    for result in processed_results:
        if len(result['objects']):
            for obj in result['objects']:
                center_x = obj['relative_coordinates']['center_x'] * relative_size['width']
                center_y = obj['relative_coordinates']['center_y'] * relative_size['height']
                width = obj['relative_coordinates']['width'] * relative_size['width']
                height = obj['relative_coordinates']['height'] * relative_size['height']
                center_x = center_x - width/2
                center_y = center_y + height/2
                height = height * (-1)
                query = connection.execute(db.insert(anomalies_table).values(rule_id = 1,frame =result['frame_id'], left_x = center_x, top_y = center_y, width = width, height = height))
                io = json.dumps(query.lastrowid)
                logging.warning(io)
                query2 = connection.execute(db.insert(video_anomalies_table).values(detected_anomaly_id = query.lastrowid , video_id = vid_id))

    info_function(100)
    logging.info("Finished Processing " + vid_name)
try:
    exec_long_running_proc("./darknet", args=["detector", "demo", "./data/obj.data", "./cfg/yolo-activity-detect.cfg", "./yolo-activity.weights", video_details['path'], "-json_port", "4050", "-dont_show", "-ext_output"])
except e:
    logging.error(e)


#./darknet detector demo ./data/obj.data ./cfg/yolo-activity-detect.cfg ./yolo-activity.weights to_be_processed/vid.mkv -json_port 4050 -dont_show -ext_output