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
        # (turn_dtm, turn_flag, lag_time, flow_response, rads, mag_bmazms, bmnums)
        list_tuple = [(dt.datetime(2013, 2, 21, 4, 2),   180, 6,   "yes",  ["fhe", "fhw", "cve"], [30, -30, 30]), 
                      (dt.datetime(2013, 5, 14, 6, 39),  180, 10,  "yes",  ["cve", "cvw"], [30, -30]),   # High background TEC, too much ground scatter
                      (dt.datetime(2013, 11, 16, 7, 24), 180, 15,  "yes",  ["fhw", "cvw"], [30, -30]), 
                      (dt.datetime(2013, 11, 16, 8, 51), 180, 6,   "yes",  ["fhe", "fhw"], [20, -20]),  
                      (dt.datetime(2014, 2, 28, 07, 24), 180, 17,  "yes",  ["bks", "cve", "ade"], [-30, 30, 30]),
                      (dt.datetime(2015, 1, 5, 3, 44),   180, 17,  "yes",  ["cve", "cvw", "ade", "adw"]),
                      (dt.datetime(2015, 1, 7, 7, 17),   180, 10,  "yes"),
                      (dt.datetime(2015, 1, 26, 11, 4),  180, 10,  "yes"),   # CVW
                      (dt.datetime(2015, 2, 1, 9, 18),   180, 10,  "yes"),   # FHE, FHW 
                      (dt.datetime(2015, 2, 5, 13, 43),  180, 10,  "yes"),   # Only ADW 
                      (dt.datetime(2015, 2, 23, 5, 36),  180, 10,  "yes"),   # BKS and CVE
                      (dt.datetime(2015, 2, 23, 8, 17),  180, 10,  "yes"),   # Only BKS
                      (dt.datetime(2015, 3, 28, 5, 02),  180, 10,  "yes"),   # Good one 
                      (dt.datetime(2015, 3, 29, 9, 49),  180, 10,  "yes"),   # Good one 
                      (dt.datetime(2015, 4, 20, 9, 40),  180, 10,  "yes"),    
                      (dt.datetime(2015, 6, 26, 7, 44),  180, 10,  "yes"),    
                      (dt.datetime(2015, 8, 23, 6, 6),   180, 10,  "yes"),    
                      (dt.datetime(2015, 9, 10, 7, 50),  180, 10,  "yes"),   # NO DATA 
                      (dt.datetime(2013, 2, 21, 6, 10),  180, 10,  "no"),   # Weak southward turning
                      (dt.datetime(2013, 5, 19, 5, 17),  180, 10,  "no"),   # Flow response is not clear
                      (dt.datetime(2014, 2, 3, 07, 20),  180, 10,  "no"),   # Very nice southward turning but no flow response
                      (dt.datetime(2014, 3, 25, 9, 7),   180, 10,  "no"),
                      (dt.datetime(2015, 1, 3, 10, 17),  180, 16,  "no"),    # Strong southward turning but no clear flow response
                      (dt.datetime(2013, 2, 21, 5, 34),  0,   17,  "yes",  ["bks", "fhe", "fhw", "cve", "cvw"], [-30, 30, -30, 30, -30]),    # response time varies at different MLT
                      (dt.datetime(2013, 11, 16, 7, 49), 0,   10,  "yes",  ["bks", "fhe", "fhw", "cve", "cvw"], [-30, 30, -30, 30, -30]),     # response time varies at different MLT, SAIS disappears near 20 MLT seen by ade, adw
                      (dt.datetime(2014, 1, 1, 8, 0),    0,   10,  "yes"), 
                      (dt.datetime(2014, 1, 3, 9, 28),   0,   10,  "yes"),   # Only BKS has good data
                      (dt.datetime(2014, 3, 1, 9, 20),   0,   10,  "yes"),   # Overshielding observed in FHE
                      (dt.datetime(2014, 12, 16, 14, 2), 0,   10,  "yes"),
                      (dt.datetime(2015, 1, 26, 10, 8),  0,   10,  "yes"),   # CVW
                      (dt.datetime(2015, 2, 1, 9, 51),   0,   10,  "yes"),   # FHE, FHW 
                      (dt.datetime(2015, 3, 16, 6, 49),  0,   10,  "yes"),   # Good one
                      (dt.datetime(2015, 3, 28, 7, 46),  0,   10,  "yes"),   
                      (dt.datetime(2015, 3, 29, 10, 22), 0,   10,  "yes"),   # Good one 
                      (dt.datetime(2015, 4, 10, 8, 39),  0,   10,  "yes"),   # BKS
                      (dt.datetime(2015, 4, 20, 10, 13), 0,   10,  "yes"),   
                      (dt.datetime(2015, 5, 10, 13, 17), 0,   10,  "yes"),   # ADW, ADE (los vel direction is the opposite to what is expected)
                      (dt.datetime(2015, 5, 19, 2, 32),  0,   10,  "yes"),   # BKS, WAL (good example for oversheilding in the premidnight sector)
                      (dt.datetime(2015, 6, 30, 5, 43),  0,   10,  "yes"),   
                      (dt.datetime(2015, 7, 28, 12, 38), 0,   10,  "yes"),   # ADW, ADE (los vel direction is the opposite to what is expected)
                      (dt.datetime(2015, 2, 8, 8, 5),    0,   10,  "no"),    
                      (dt.datetime(2015, 6, 3, 10, 48),  0,   10,  "no"),
                      (dt.datetime(2015, 7, 9, 4, 57),   0,   10,  "no")]


        turn_dtms = [x[0] for x in list_tuple]
        turn_flag = [x[1] for x in list_tuple]
        # Deley in convection response
        lag_time = [x[2] for x in list_tuple]

        df = pd.DataFrame(data={"datetime":turn_dtms, "turn_flag":turn_flag,
                          "lag_time":lag_time})
        if IMF_turning != "all":
            if IMF_turning == "northward":
                df = df.loc[df.turn_flag==0, :] 
            if IMF_turning == "southward":
                df = df.loc[df.turn_flag==180, :] 


    # Creat AU, AL turning event list
    if AUAL_events:
        # Create tuples
        # (turn_dtm, turn_flag, lag_time)
        list_tuple = [(dt.datetime(2015, 1, 11, 6, 30),   "AL",      1), 
                      (dt.datetime(2015, 4, 3, 8, 47),    "AL",      1),
                      (dt.datetime(2015, 7, 22, 10, 5),   "AL,AU",   1)]    # SAIS to SAPS


    return df

if __name__ == "__main__":
    #df = create_event_list(IMF_turning="southward") 
    df = create_event_list(IMF_turning="northward") 
