import os
import sys
import requests
import json
import sqlalchemy as db
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base


#Get id of the video to be processed.
args = sys.argv
vid_id = args[0]

DB_PATH = os.environ['DB_PATH']

#If environment DB_PATH is set use that engine instead
if DB_PATH:
    engine = create_engine(DB_PATH, echo=True)
else:
    engine = create_engine('sqlite:///anomaly.db', echo=True)
Base = declarative_base()

# Check if table exists if not create it.
#########################################
class DetectedActivity(Base):
    #Database Schema
    __tablename__ = "DetectedAnomalies"

    id = Column(Integer, primary_key=True)
    RuleId = Column(String)
    frame_id = Column(Integer)
    center_x = Column(Float)
    center_y = Column(Float)
    width = Column(Float)
    height = Column(Float)

    def __init__(self, name):

        self.name = name    


Base.metadata.create_all(engine)
connection = engine.connect()
metadata = db.MetaData()
anomalies = db.Table('DetectedAnomalies', metadata, autoload=True, autoload_with=engine)
#Check with sql statement and get the whole stream of video.
video_table = db.Table('Videos',metadata,autoload=True, autoload_with=engine)

video_details = connection.execute(db.select([video_table]).where(video_table._columns.VideoId == vid_id))
vid_name = video_details['Name']
# ffmpeg -i input -vf scale=iw/2:-1 output
#r = requests.get('http://34.73.60.123:8070/')
r = requests.get('https://api.myjson.com/bins/1aajyi')
processed_results = r.json()
for result in processed_results:
    if len(result['objects']):
        for obj in result['objects']:
            query = db.insert(anomalies).values(detected_anomaly = "Running Person", frame_id=result['frame_id'], center_x = obj['relative_coordinates']['center_x'], center_y = obj['relative_coordinates']['center_y'], width = obj['relative_coordinates']['width'], height = obj['relative_coordinates']['height']) 
            ResultProxy = connection.execute(query)

print("Finished Processing " + vid_name)