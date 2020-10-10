import json
import pandas as pd
import numpy as np

def odasdf(odData =None):
    
    fold_list=[]
    main_obj =[]
    if odData['video']['folder'] not in fold_list:
        fold_list.append(odData['video']['folder'])
        main_obj.append(odData)

    odData = main_obj

    objectList = []

    allFrames = []

    for frame in odData[0]['ml-data']['object-detection']['frames']:
        for obj in frame['objects']:
            obj['frame'] = frame['frame']
            if obj['score'] > 0.8:
                allFrames.append(obj)
    for item in allFrames:
        item['folder'] = odData[0]['video']['folder']
    
    objectList.append(allFrames)

    flat_list = [item for sublist in objectList for item in sublist]
    odDf = pd.DataFrame(flat_list)
    odDf = odDf.sort_values(by=['folder', 'frame'])
    return odDf
