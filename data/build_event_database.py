import pandas as pd
import numpy as np
import datetime as dt

def build_event_database(IMF_turning="northward", event_status="good",
                         radar_response="yes"):

    """Builds a dataframe for events of interest

    Parameters:
    ----------
    IMF_turning : str
        Can be "northward", "southward, or "all"

    """
    # Northward Turning Events
    events = [{"datetime": dt.datetime(2013, 2, 21, 5, 34),   "rad":"cve",  "bmnum":7,   "lag_time":20,  "turn_flag":0,    "mlt":21,   "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":None}, 
              {"datetime": dt.datetime(2013, 2, 21, 5, 34),   "rad":"cvw",  "bmnum":13,  "lag_time":20,  "turn_flag":0,    "mlt":21,   "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None}, 
              {"datetime": dt.datetime(2013, 2, 21, 5, 34),   "rad":"fhe",  "bmnum":7,   "lag_time":20,  "turn_flag":0,    "mlt":23,   "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":None}, 
              {"datetime": dt.datetime(2013, 2, 21, 5, 34),   "rad":"fhw",  "bmnum":13,  "lag_time":20,  "turn_flag":0,    "mlt":23,   "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2013, 2, 21, 5, 34),   "rad":"bks",  "bmnum":13,  "lag_time":20,  "turn_flag":0,    "mlt":23,   "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"adw",  "bmnum":13,  "lag_time":15,  "turn_flag":0,    "mlt":21,   "bmdir":"west",   "event_status":"bad",   "same_lagtime":"yes",  "comment":"Confusing, not enough data"},
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"cve",  "bmnum":1,   "lag_time":15,  "turn_flag":0,    "mlt":0,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"cvw",  "bmnum":19,  "lag_time":15,  "turn_flag":0,    "mlt":0,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"fhe",  "bmnum":7,   "lag_time":15,  "turn_flag":0,    "mlt":3,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":"Oposite response"}, 
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"fhw",  "bmnum":13,  "lag_time":15,  "turn_flag":0,    "mlt":3,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2015, 2, 1, 9, 51),    "rad":"bks",  "bmnum":13,  "lag_time":15,  "turn_flag":0,    "mlt":5,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2013, 11, 16, 7, 49),  "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":23,   "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2013, 11, 16, 7, 49),  "rad":"cve",  "bmnum":1,   "lag_time":10,  "turn_flag":0,    "mlt":0,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2013, 11, 16, 7, 49),  "rad":"fhe",  "bmnum":7,   "lag_time":10,  "turn_flag":0,    "mlt":1,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":"Confusing"},
              {"datetime": dt.datetime(2013, 11, 16, 7, 49),  "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":1,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2013, 11, 16, 7, 49),  "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":3,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2014, 1, 3, 9, 28),    "rad":"cve",  "bmnum":1,   "lag_time":21,  "turn_flag":0,    "mlt":0,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2014, 1, 3, 9, 28),    "rad":"adw",  "bmnum":13,  "lag_time":21,  "turn_flag":0,    "mlt":21,   "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":"Confusing"}, 
              {"datetime": dt.datetime(2014, 1, 3, 9, 28),    "rad":"cvw",  "bmnum":19,  "lag_time":21,  "turn_flag":0,    "mlt":0,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"cve",  "bmnum":1,   "lag_time":15,  "turn_flag":0,    "mlt":0,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"cve",  "bmnum":7,   "lag_time":15,  "turn_flag":0,    "mlt":1,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":"SAPS to SAIS after IMF turning"},
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"cvw",  "bmnum":13,  "lag_time":15,  "turn_flag":0,    "mlt":1,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None}, 
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"fhe",  "bmnum":7,   "lag_time":15,  "turn_flag":0,    "mlt":3,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":"Response Not clear"},
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"fhw",  "bmnum":13,  "lag_time":15,  "turn_flag":0,    "mlt":3,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2015, 1, 26, 10, 8),   "rad":"bks",  "bmnum":13,  "lag_time":15,  "turn_flag":0,    "mlt":5,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2014, 1, 1, 8, 0),     "rad":"fhe",  "bmnum":1,   "lag_time":12,  "turn_flag":0,    "mlt":0,    "bmdir":"east",   "event_status":"bad",   "same_lagtime":"yes",  "comment":"Response not clear"}, 
              {"datetime": dt.datetime(2014, 1, 1, 8, 0),     "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":1,    "bmdir":"west",   "event_status":"bad",   "same_lagtime":"yes",  "comment":"Response not clear"},
              {"datetime": dt.datetime(2014, 3, 1, 9, 20),    "rad":"cve",  "bmnum":7,   "lag_time":22,  "turn_flag":0,    "mlt":1,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":None}, 
              {"datetime": dt.datetime(2014, 3, 1, 9, 20),    "rad":"fhw",  "bmnum":13,  "lag_time":22,  "turn_flag":0,    "mlt":1,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2014, 3, 1, 9, 20),    "rad":"fhe",  "bmnum":7,   "lag_time":22,  "turn_flag":0,    "mlt":3,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2014, 3, 1, 9, 20),    "rad":"bks",  "bmnum":13,  "lag_time":22,  "turn_flag":0,    "mlt":3,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"yes",  "comment":None},
              {"datetime": dt.datetime(2014, 12, 16, 14, 02), "rad":"adw",  "bmnum":13,  "lag_time":15,  "turn_flag":0,    "mlt":1,    "bmdir":"west",   "event_status":"good",  "same_lagtime":"no",   "comment":None},
              {"datetime": dt.datetime(2014, 12, 16, 14, 02), "rad":"ade",  "bmnum":7,   "lag_time":15,  "turn_flag":0,    "mlt":3,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"no",   "comment":None},
              {"datetime": dt.datetime(2014, 12, 16, 14, 02), "rad":"cve",  "bmnum":7,   "lag_time":15,  "turn_flag":0,    "mlt":5,    "bmdir":"east",   "event_status":"good",  "same_lagtime":"no",   "comment":None},
              {"datetime": dt.datetime(2015, 10, 3, 5, 28),   "rad":"ade",  "bmnum":7,   "lag_time":10,  "turn_flag":0,    "mlt":19,   "bmdir":"east",   "event_status":None,    "same_lagtime":"yes",  "comment":"Short lived, very clear overshielding in premidnight sector"},
              {"datetime": dt.datetime(2015, 10, 3, 5, 28),   "rad":"cve",  "bmnum":7,   "lag_time":10,  "turn_flag":0,    "mlt":22,   "bmdir":"east",   "event_status":None,    "same_lagtime":"yes",  "comment":"Short lived, very clear overshielding in premidnight sector"},
              {"datetime": dt.datetime(2015, 10, 3, 5, 28),   "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":19,   "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"Short lived, response is not very clear"},
              {"datetime": dt.datetime(2015, 10, 3, 5, 28),   "rad":"adw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":19,   "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"Short lived, very clear overshielding in premidnight sector"},
              {"datetime": dt.datetime(2015, 10, 18, 5, 38),  "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":21,   "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"clear overshielding response in bks data, check for other radars for this turning"},
              {"datetime": dt.datetime(2015, 11, 16, 6, 4),   "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"clear overshielding response in bks data, not enough coverage from other radars"},
              {"datetime": dt.datetime(2015, 12, 3, 14, 14),  "rad":"adw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"not a good turning, clear overshielding response in adw data, check for other radars"},
              {"datetime": dt.datetime(2015, 12, 7, 7, 58),   "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"Short lived, very clear overshielding in several radars"},
              {"datetime": dt.datetime(2015, 12, 22, 11, 36), "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"clear overshielding in cvw, check for other radars"},
              {"datetime": dt.datetime(2015, 12, 25, 7, 51),  "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"slow northward turning, clear overshielding in bks, check for other radars"},
              {"datetime": dt.datetime(2016, 2, 16, 9, 47),   "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"slow northward turning, good data coverage, good flow response in cvw, check for other radars"},
              {"datetime": dt.datetime(2014, 11, 21, 9, 43),  "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"northward turning, good data coverage, good flow response in fhw, check for other radars"},
              {"datetime": dt.datetime(2014, 12, 7, 10, 40),  "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"northward turning, good data coverage, good flow response in cvw, check for other radars"},
              {"datetime": dt.datetime(2015, 1, 3, 5, 50),    "rad":"bks",  "bmnum":7,   "lag_time":10,  "turn_flag":0,    "mlt":19,   "bmdir":"east",   "event_status":None,    "same_lagtime":"yes",  "comment":"Short lived, very clear overshielding in bks, check for other radars"},
              {"datetime": dt.datetime(2014, 2, 8, 7, 50),    "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"northward turning, good data coverage, good flow response in fhw, check for other radars"},
              {"datetime": dt.datetime(2013, 11, 11, 14, 13), "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"northward turning, good data coverage, good flow response in cvw, check for other radars"},
              {"datetime": dt.datetime(2013, 2, 8, 3, 53),    "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"very nich northward turning, good data coverage, good flow response in cvw, check for other radars"},
              {"datetime": dt.datetime(2012, 11, 20, 8, 46),  "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"very nich northward turning, good data coverage, good flow response in cvw, check for other radars"},
              {"datetime": dt.datetime(2012, 12, 2, 8, 47),   "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"slow northward turning, good data coverage, good flow response in bks, check for other radars"}]

              # Northward turning but no flow response
    events = events + [\
              {"datetime": dt.datetime(2014, 11, 20, 6, 35),  "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"northward turning, but no flow response, check for other radars"},
              {"datetime": dt.datetime(2013, 11, 8, 6, 3),    "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"northward turning, but no flow response, check for other radars"},
              {"datetime": dt.datetime(2013, 2, 7, 14, 24),   "rad":"adw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"northward turning, but no flow response, check for other radars"},
              {"datetime": dt.datetime(2013, 2, 20, 3, 45),   "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"northward turning, but no flow response, check for other radars"}]
              #{"datetime": dt.datetime(2016, 2, 23, 6, 15),   "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":0,    "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"nice northward turning, but no flow response"},

              # Southward turning
    events = events + [\
              {"datetime": dt.datetime(2015, 10, 5, 4, 57),   "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":19,   "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"slow turning, Response not clear, check for other radars for this turning"},
              {"datetime": dt.datetime(2015, 11, 17, 8, 0),   "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"nice southward turning, clear undershielding response in fhw data, not enough coverage from other radars"},
              {"datetime": dt.datetime(2015, 11, 18, 13, 50), "rad":"ade",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"east",   "event_status":None,    "same_lagtime":"yes",  "comment":"nice southward turning, clear undershielding response in ade, adw data"},
              {"datetime": dt.datetime(2015, 11, 18, 13, 50), "rad":"adw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"nice southward turning, clear undershielding response in ade, adw data"},
              {"datetime": dt.datetime(2015, 11, 29, 8, 40),  "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"strong southward turning, clear undershielding response in bks, fhe, fhw. check for other radars"},
              {"datetime": dt.datetime(2015, 12, 2, 8, 5),    "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"strong southward turning, clear undershielding response in bks, cvw, cve, fhe, fhw. check for other radars"},
              {"datetime": dt.datetime(2015, 12, 5, 15, 0),   "rad":"ade",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"clear undershielding response in ade, adw. check for other radars"},
              {"datetime": dt.datetime(2015, 12, 7, 7, 40),   "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"Short lived, clear undershielding in several radars"},
              {"datetime": dt.datetime(2015, 12, 28, 9, 5),   "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"clear undershielding in fhw, check for other radars"},
              {"datetime": dt.datetime(2016, 1, 10, 13, 50),  "rad":"adw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"undershielding reponse in ade, adw"},
              {"datetime": dt.datetime(2016, 1, 17, 11, 5),   "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"very nice southward turning, undershielding reponse in fhw, check for other radars"},
              {"datetime": dt.datetime(2016, 1, 23, 7, 40),   "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"undershielding reponse in bks, check for other radars"},
              {"datetime": dt.datetime(2016, 2, 2, 14, 0),    "rad":"adw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"weak undershielding reponse in adw"},
              {"datetime": dt.datetime(2016, 2, 21, 4, 0),    "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"weak reponse in fhe, fhw, cvw. check for other radars"},
              {"datetime": dt.datetime(2014, 12, 7, 10, 5),   "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"good reponse in cvw. check for other radars"},
              {"datetime": dt.datetime(2014, 2, 28, 7, 25),   "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"good reponse in bks. check for other radars"},
              {"datetime": dt.datetime(2014, 3, 1, 10, 0),    "rad":"fhw",  "bmnum":7,   "lag_time":22,  "turn_flag":180,  "mlt":1,    "bmdir":"east",   "event_status":None,    "same_lagtime":"yes",  "comment":"good reponse in fhw. check for other radars"}, 
              {"datetime": dt.datetime(2013, 2, 20, 4, 20),   "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"northward turning, flow response in cvw, check for other radars"}]

              # Southward turning but no flow response
    events = events + [\
              {"datetime": dt.datetime(2016, 1, 17, 11, 5),   "rad":"fhw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"nice southward turning, no flow response"},
              {"datetime": dt.datetime(2016, 2, 13, 5, 30),   "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"southward turning, no flow response"},
              {"datetime": dt.datetime(2014, 11, 19, 9, 0),   "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"nice southward turning, but no flow response, check for other radars"},
              {"datetime": dt.datetime(2014, 11, 19, 11, 5),  "rad":"bks",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"nice southward turning, but no flow response, check for other radars"},
              {"datetime": dt.datetime(2013, 11, 8, 5, 30),   "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"southward turning, but no flow response, check for other radars"},
              {"datetime": dt.datetime(2013, 2, 7, 10, 55),   "rad":"cvw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"southward turning, but no flow response, check for other radars"},
              {"datetime": dt.datetime(2013, 2, 28, 6, 45),   "rad":"adw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"southward turning, but no flow response, check for other radars"},
              {"datetime": dt.datetime(2012, 11, 21, 3, 40),  "rad":"adw",  "bmnum":13,  "lag_time":10,  "turn_flag":180,  "mlt":None, "bmdir":"west",   "event_status":None,    "same_lagtime":"yes",  "comment":"southward turning, but no flow response, check for other radars"}]

    data_dict = {}
    keys = events[0].keys()
    for key in keys:
        data_dict[key] = [x[key] for x in events]
    df = pd.DataFrame(data=data_dict)

    # Select events based on status
    if event_status != "all":
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
    #IMF_turning = "all"
    IMF_turning="northward"
    event_status="all"
    #event_status="good"
    df = build_event_database(IMF_turning=IMF_turning, event_status=event_status)

