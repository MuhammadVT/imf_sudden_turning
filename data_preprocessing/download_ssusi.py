import os
import datetime as dt
from imagers.ssusi import dwnld_ssusi
import sys
sys.path.append("../data/")
from create_event_list import create_event_list

# Create event list
IMF_turning = "all"
IMF_events = True
df_turn = create_event_list(IMF_events=IMF_events, IMF_turning=IMF_turning)

# Find the convection respond time
turn_dtms = [x.to_pydatetime() for x in df_turn.datetime]
event_dates = [dt.datetime(x.year, x.month, x.day) for x in turn_dtms]

# Download SSUSI data
#dataTypeList = [ "sdr" ]
dataTypeList = [ "sdr", "edr-aur" ]
#dataTypeList = [ "edr-aur" ]
satList = ["f16", "f17", "f18", "f19"]
tempFileDir = "../data/ssusi/"# Make sure you have this dir or create it
#tempFileDir = "../../data/ssusi/stat5214G_project/"# Make sure you have this dir or create it

ssDwnldObj = dwnld_ssusi.SSUSIDownload(outBaseDir = tempFileDir)
for currDate in event_dates:
    print "currently downloading files for --> ",\
        currDate.strftime("%Y-%m-%d")
    ssDwnldObj.download_files(currDate, dataTypeList)


