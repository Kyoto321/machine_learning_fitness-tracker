import pandas as pd
from glob import glob

single_file_accl = pd.read_csv('../../data/raw/MetaMotion/MetaMotion/A-bench-heavy2-rpe8_MetaWear_2019-01-11T16.10.08.270_C42732BE255C_Accelerometer_12.500Hz_1.4.4.csv')

single_file_gyr = pd.read_csv('../../data/raw/MetaMotion/MetaMotion/A-bench-heavy2-rpe8_MetaWear_2019-01-11T16.10.08.270_C42732BE255C_Gyroscope_25.000Hz_1.4.4.csv')

#print(single_file_gyr)

# list all files
files = glob("../../data/raw/MetaMotion/MetaMotion/*csv")
len(files)

files[0]

# extract features from file 
data_path = "../../data/raw/MetaMotion/MetaMotion/"
f = files[0]

participant = f.split("-")[0].replace(data_path, "")
label = f.split("-")[1]
category = f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019")

df = pd.read_csv(f)

df["participant"] = participant
df["label"] = label
df["category"] = category

# read all files
accl_df = pd.DataFrame()
gyr_df = pd.DataFrame()

# to create unique identifier
accl_set = 1
gyr_set = 1

for f in files:
    # extract the features
    participant = f.split("-")[0].replace(data_path, "")
    label = f.split("-")[1]
    category = f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019")

    # read into dataframe
    df = pd.read_csv(f)
    
    # add columns to the dataframe
    df["participant"] = participant
    df["label"] = label
    df["category"] = category
    
    if "Accelerometer" in f:
        df["set"] = accl_set # remove NaN for set values
        accl_set += 1
        accl_df = pd.concat([accl_df, df])
        
    if "Gyroscope" in f:
        df["set"] = gyr_set # remove NaN for set values
        gyr_set += 1
        gyr_df = pd.concat([gyr_df, df])
        

# working with datatime
pd.to_datetime(accl_df["epoch (ms)"], unit="ms")

accl_df.index = pd.to_datetime(accl_df["epoch (ms)"], unit="ms")
gyr_df.index = pd.to_datetime(gyr_df["epoch (ms)"], unit="ms")


# remove column
del accl_df["epoch (ms)"]
del accl_df["time (01:00)"]
del accl_df["elapsed (s)"]

del gyr_df["epoch (ms)"]
del gyr_df["time (01:00)"]
del gyr_df["elapsed (s)"]


### Create a function

files = glob("../../data/raw/MetaMotion/MetaMotion/*csv")

def read_data_from_files(files):
    # read all files
    accl_df = pd.DataFrame()
    gyr_df = pd.DataFrame()

    # to create unique identifier
    accl_set = 1
    gyr_set = 1

    for f in files:
        # extract the features
        participant = f.split("-")[0].replace(data_path, "")
        label = f.split("-")[1]
        category = f.split("-")[2].rstrip("123").rstrip("_MetaWear_2019")

        # read into dataframe
        df = pd.read_csv(f)
        
        # add columns to the dataframe
        df["participant"] = participant
        df["label"] = label
        df["category"] = category
        
        if "Accelerometer" in f:
            df["set"] = accl_set # remove NaN for set values
            accl_set += 1
            accl_df = pd.concat([accl_df, df])
            
        if "Gyroscope" in f:
            df["set"] = gyr_set # remove NaN for set values
            gyr_set += 1
            gyr_df = pd.concat([gyr_df, df])
            
            
    accl_df.index = pd.to_datetime(accl_df["epoch (ms)"], unit="ms")
    gyr_df.index = pd.to_datetime(gyr_df["epoch (ms)"], unit="ms")


    # remove column
    del accl_df["epoch (ms)"]
    del accl_df["time (01:00)"]
    del accl_df["elapsed (s)"]

    del gyr_df["epoch (ms)"]
    del gyr_df["time (01:00)"]
    del gyr_df["elapsed (s)"]        
            
    return accl_df, gyr_df

accl_df, gyr_df = read_data_from_files(files)
            
            
            
### Merging datasets ###
merged_data = pd.concat([accl_df.iloc[:,:3], gyr_df], axis=1)

merged_data.columns = ["accl_x", "accl_y", "accl_z", "gyr_x", "gyr_y", "gyr_z", "label", "category", "participant", "set",]


### To balance the measurement using pandas frequency (resemble) method
# only work on time series dataframe

sampling = {"accl_x": "mean", "accl_y":"mean", "accl_z":"mean", "gyr_x":"mean", "gyr_y":"mean", "gyr_z":"mean", "label":"last", "category":"last", "participant":"last", "set":"last",}

merged_data[:1000].resample(rule="200ms").apply(sampling)

# loop over each day with resample method
days = [g for n, g in merged_data.groupby(pd.Grouper(freq="b"))]

resampled_data = pd.concat([df.resample(rule="200ms").apply(sampling).dropna() for df in days])

resampled_data.info()

resampled_data["set"] = resampled_data["set"].astype("int")


### Export Data
resampled_data.to_pickle("../../data/works/01_processed_data.pkl")

