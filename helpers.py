import random
from csv import writer

# Unit tested by writing simple lines and manually verifying that output in csv is expected value
def write_data(filename, data):
    with open(filename, 'a') as log:
        writer_object = writer(log)
        writer_object.writerow(data)
        log.close()
    return data

# Initializes log file for each process and returns (log file title '<simple_pid>_<clock_rate>_log.csv', clock_rate) pair,
# where simple_pid \in {1, 2, 3}.
def init_log(conPort, portDict):
    if conPort == portDict['port1']:
        simple_pid = '1'
    elif conPort == portDict['port2']:
        simple_pid = '2'
    else:
        simple_pid = '3' 
    clock_rate = random.randint(1,6)
    filename = simple_pid + '_' + str(clock_rate) + "_log.csv"
    # create log file
    write_data(filename, ["Event Type", "Global Time", "Queue Length", "Logical Clock Time"])
    return filename, clock_rate
