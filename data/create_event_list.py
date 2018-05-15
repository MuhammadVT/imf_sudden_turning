def create_event_list(IMF_events=True, AUAL_events=False,
                      IMF_turning="northward"): 
    """
    Parameters
    IMF_turing : str
        0 for northward, 90 for duskward,
        180 for southward turning, 270 dawnward
    """
    import datetime as dt
    import pandas as pd
    
    # Creat IMF turning event list
    if IMF_events:
        # Create tuples
        # (turn_dtm, turn_flag, lag_time, flow_response)
        list_tuple = [(dt.datetime(2013, 2, 21, 4, 2),   180, 10,  "yes"), 
                      (dt.datetime(2013, 2, 21, 6, 10),  180, 10,  "yes"),   # Weak southward turning
                      (dt.datetime(2013, 5, 14, 6, 39),  180, 10,  "yes"),
                      (dt.datetime(2013, 5, 19, 5, 17),  180, 10,  "yes"),
                      (dt.datetime(2013, 11, 16, 7, 25), 180, 14,  "yes"), 
                      (dt.datetime(2013, 11, 16, 8, 51), 180, 6,   "yes"),  
                      (dt.datetime(2014, 2, 3, 07, 20),  180, 10,  "yes"),
                      (dt.datetime(2014, 2, 28, 07, 20), 180, 10,  "yes"),
                      (dt.datetime(2015, 1, 3, 10, 17),  180, 10,  "yes"),
                      (dt.datetime(2015, 1, 5, 3, 45),   180, 10,  "yes"),
                      (dt.datetime(2015, 1, 7, 7, 17),   180, 10,  "yes"),
                      (dt.datetime(2015, 1, 26, 11, 4),  180, 10,  "yes"),   # CVW
                      (dt.datetime(2015, 2, 1, 9, 18),   180, 10,  "yes"),   # FHE, FHW 
                      (dt.datetime(2015, 2, 5, 13, 43),  180, 10,  "yes"),   # Only ADW 
                      (dt.datetime(2015, 2, 23, 5, 36),  180, 10,  "yes"),   # BKS and CVE
                      (dt.datetime(2015, 2, 23, 8, 17),  180, 10,  "yes"),   # Only BKS
                      (dt.datetime(2015, 3, 28, 5, 02),  180, 10,  "yes"),   # Good one 
                      (dt.datetime(2014, 3, 25, 9, 7),   180, 10,  "no"),
                      (dt.datetime(2013, 2, 21, 5, 33),  0,   10,  "yes"),
                      (dt.datetime(2013, 11, 16, 7, 47), 0,   10,  "yes"), 
                      (dt.datetime(2014, 1, 1, 8, 0),    0,   10,  "yes"), 
                      (dt.datetime(2014, 1, 3, 9, 28),   0,   10,  "yes"),   # Only BKS has good data
                      (dt.datetime(2014, 3, 1, 9, 20),   0,   10,  "yes"),   # Overshielding observed in FHE
                      (dt.datetime(2014, 12, 16, 14, 2), 0,   10,  "yes"),
                      (dt.datetime(2015, 1, 26, 10, 8),  0,   10,  "yes"),   # CVW
                      (dt.datetime(2015, 2, 1, 9, 51),   0,   10,  "yes"),   # FHE, FHW 
                      (dt.datetime(2015, 3, 16, 6, 49),  0,   10,  "yes"),   # Good one
                      (dt.datetime(2015, 3, 28, 7, 46),  0,   10,  "yes"),   
                      (dt.datetime(2015, 2, 8, 8, 5),    0,   10,  "no")]    


        turn_dtms = [x[0] for x in list_tuple]
        turn_flag = [x[1] for x in list_tuple]
        # Deley in convection response
        lag_time = [x[2] for x in list_tuple]

        df = pd.DataFrame(data={"datetime":turn_dtms, "turn_flag":turn_flag,
                          "lag_time":lag_time})
        if IMF_turning == "northward":
            df = df.loc[df.turn_flag==0, :] 
        if IMF_turning == "southward":
            df = df.loc[df.turn_flag==180, :] 


    # Creat AU, AL turning event list
    if AUAL_events:
        # Create tuples
        # (turn_dtm, turn_flag, lag_time)
        list_tuple = [(dt.datetime(2015, 1, 11, 6, 30),   "AL", 1), 
                      (dt.datetime(2014, 12, 16, 14, 2), 0,   1)]


    return df

if __name__ == "__main__":
    #df = create_event_list(IMF_turning="southward") 
    df = create_event_list(IMF_turning="northward") 
