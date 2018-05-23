import pandas as pd
import numpy as np
import datetime as dt

def build_event_database(IMF_turning="northward", event_status="good"):

    """Builds a dataframe for events of interest

    Parameters:
    ----------
    IMF_turning : str
        Can be "northward", "southward, or "all"

    """
    # Northward Turning Events
    events = [{"datetime": dt.datetime(2013, 2, 21, 5, 34),   "rad":"cve",  "bmnum":7,   "lag_time":10,  "turn_flag":0,  "mlt":21,  "bmdir":"east",   "event_status":"good", "comment":None}, 
              {"datetime": dt.datetime(2013, 2, 21, 5, 34),   "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,  "mlt":21,  "bmdir":"west",   "event_status":"good", "comment":None}, 
              {"datetime": dt.datetime(2013, 2, 21, 5, 34),   "rad":"fhe",  "bmnum":7,   "lag_time":15,  "turn_flag":0,  "mlt":23,  "bmdir":"east",   "event_status":"good", "comment":None}, 
              {"datetime": dt.datetime(2013, 2, 21, 5, 34),   "rad":"fhw",  "bmnum":13,  "lag_time":21,  "turn_flag":0,  "mlt":23,  "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2013, 2, 21, 5, 34),   "rad":"bks",  "bmnum":19,  "lag_time":10,  "turn_flag":0,  "mlt":0,   "bmdir":"west",   "event_status":"bad",  "comment":"Confusing"},
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"adw",  "bmnum":13,  "lag_time":20,  "turn_flag":0,  "mlt":21,  "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"cve",  "bmnum":1,   "lag_time":15,  "turn_flag":0,  "mlt":0,   "bmdir":"east",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"cvw",  "bmnum":19,  "lag_time":21,  "turn_flag":0,  "mlt":0,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"fhe",  "bmnum":7,   "lag_time":21,  "turn_flag":0,  "mlt":3,   "bmdir":"east",   "event_status":"good", "comment":None}, 
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"fhw",  "bmnum":13,  "lag_time":16,  "turn_flag":0,  "mlt":3,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"bks",  "bmnum":13,  "lag_time":15,  "turn_flag":0,  "mlt":5,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2013, 11, 16, 7, 49),  "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,  "mlt":23,  "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2013, 11, 16, 7, 49),  "rad":"cve",  "bmnum":1,   "lag_time":10,  "turn_flag":0,  "mlt":0,   "bmdir":"east",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2013, 11, 16, 7, 49),  "rad":"fhe",  "bmnum":7,   "lag_time":20,  "turn_flag":0,  "mlt":1,   "bmdir":"east",   "event_status":"bad",  "comment":"Confusing"},
              {"datetime": dt.datetime(2013, 11, 16, 7, 49),  "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,  "mlt":1,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2013, 11, 16, 7, 49),  "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":0,  "mlt":3,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2014, 1, 3, 9, 28),    "rad":"cve",  "bmnum":1,   "lag_time":21,  "turn_flag":0,  "mlt":0,   "bmdir":"east",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2014, 1, 3, 9, 28),    "rad":"adw",  "bmnum":18,  "lag_time":10,  "turn_flag":0,  "mlt":21,  "bmdir":"west",   "event_status":"bad",  "comment":"Confusing"}, 
              {"datetime": dt.datetime(2014, 1, 3, 9, 28),    "rad":"cvw",  "bmnum":19,  "lag_time":9,   "turn_flag":0,  "mlt":0,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"cve",  "bmnum":1,   "lag_time":12,  "turn_flag":0,  "mlt":0,   "bmdir":"east",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"cve",  "bmnum":7,   "lag_time":10,  "turn_flag":0,  "mlt":1,   "bmdir":"east",   "event_status":"bad",  "comment":"SAPS to SAIS after IMF turning"},
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"cvw",  "bmnum":13,  "lag_time":15,  "turn_flag":0,  "mlt":1,   "bmdir":"west",   "event_status":"good", "comment":None}, 
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"fhe",  "bmnum":7,   "lag_time":10,  "turn_flag":0,  "mlt":3,   "bmdir":"east",   "event_status":"good", "comment":"Not clear"},
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"fhw",  "bmnum":13,  "lag_time":20,  "turn_flag":0,  "mlt":3,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"bks",  "bmnum":13,  "lag_time":15,  "turn_flag":0,  "mlt":5,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2014, 1, 1, 8, 00),    "rad":"fhe",  "bmnum":7,   "lag_time":12,  "turn_flag":0,  "mlt":1,   "bmdir":"east",   "event_status":"bad",  "comment":"Confusing"}, 
              {"datetime": dt.datetime(2014, 1, 1, 8, 00),    "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,  "mlt":1,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2014, 3, 1, 9, 20),    "rad":"cve",  "bmnum":7,   "lag_time":20,  "turn_flag":0,  "mlt":1,   "bmdir":"east",   "event_status":"bad",  "comment":"Confusing"}, 
              {"datetime": dt.datetime(2014, 3, 1, 9, 20),    "rad":"fhw",  "bmnum":13,  "lag_time":12,  "turn_flag":0,  "mlt":1,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2014, 3, 1, 9, 20),    "rad":"fhe",  "bmnum":7,   "lag_time":22,  "turn_flag":0,  "mlt":3,   "bmdir":"east",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2014, 3, 1, 9, 20),    "rad":"bks",  "bmnum":13,  "lag_time":22,  "turn_flag":0,  "mlt":3,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2014, 12, 16, 14, 02), "rad":"adw",  "bmnum":13,  "lag_time":15,  "turn_flag":0,  "mlt":1,   "bmdir":"west",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2014, 12, 16, 14, 02), "rad":"ade",  "bmnum":7,   "lag_time":17,  "turn_flag":0,  "mlt":3,   "bmdir":"east",   "event_status":"good", "comment":None},
              {"datetime": dt.datetime(2014, 12, 16, 14, 02), "rad":"cve",  "bmnum":7,   "lag_time":10,  "turn_flag":0,  "mlt":5,   "bmdir":"east",   "event_status":"good", "comment":None}]

    data_dict = {}
    keys = events[0].keys()
    for key in keys:
        data_dict[key] = [x[key] for x in events]
    df = pd.DataFrame(data=data_dict)

    # Select events based on status
    df = df.loc[df.event_status == event_status,:]

    # Select events based on IMF turnings
    if IMF_turning != "all":
        if IMF_turning == "northward":
            df = df.loc[df.turn_flag==0, :]
        if IMF_turning == "southward":
            df = df.loc[df.turn_flag==180, :]


    return df
#    df = pd.DataFrame(data={"datetime":[x["event_dtm"] for x in events],
#                            "rad": [x["rad"] for x in events],
#                            "bmnum":[x["bmnum"] for x in events],
#                            "lag_time":[x["lag_time"] for x in events],
#                            "turn_flag":[x["turn_flag"] for x in events],
#                            "mlt":[x["mlt"] for x in events],
#                            "bmdir":[x["bmdir"] for x in events]})
#
if __name__ == "__main__":
    df = build_event_database(IMF_turning="northward", event_status="good")

