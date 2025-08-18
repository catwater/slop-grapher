import reader
import matplotlib.pyplot as plt

data_path = "Online Data"

data = reader.load_files(data_path)
data['Time in seconds'] = data['Timestamp'].astype("int64") // 10**9
data['Elapsed Time (Seconds)'] = (data['Time in seconds'] - data['Time in seconds'][0])
data['Time Diff (min)'] = data['Elapsed Time (Seconds)'].diff() / 60
data = data.loc[data['Time Diff (min)'] <= 15]
data = data.loc[(data['PS Voltage (Volts)'] > 74) & (data['PS Voltage (Volts)'] < 100)]
data['Cumulative Time (hrs)'] = data['Time Diff (min)'].cumsum() / 60
data = data.loc[data['Cumulative Time (hrs)'] > 00]

plt.scatter(data['Cumulative Time (hrs)'], data['PS Voltage (Volts)'],s=3)
plt.show()