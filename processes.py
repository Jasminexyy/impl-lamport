from multiprocessing import Process
import os
import socket
from _thread import *
import threading
import random
import time
from threading import Thread
import queue

import helpers



# Send a message based on roll of 1-10
def send_roll(log, config, s1, s2):
    # Send: 1 -> 2, 2 -> 3, or 3 -> 1
    global logic_clock
    global net_q
    # Send to both
    if roll_10 == 3:
        data = send_request(log,config,s1,s2)
    # 4-10: Internal event
    else:
        data = helpers.write_data(log, ["Internal", str(time.time() - START_TIME), "0", str(logic_clock)])
    return data

def init_threads(log, config):
    global net_q
    # Initialize server and consumers
    server_thread = Thread(target=init_server, args=(log, config))
    server_thread.start()
    time.sleep(2)

    # Initialize producers
    prod_thread = Thread(target=producer, args=(log, config, config[2], config[3]))
    prod_thread.start()
    time.sleep(2)

# Initialize server and consumers at sPort
def init_server(log,config):
    global net_q
    HOST = str(config[0])
    PORT = int(config[1])

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen()
    while True:
        conn, addr = s.accept()
        start_new_thread(consumer, (log,conn, PORT))

def consumer(log, conn, PORT):
    sleepVal = 0.0
    host = "127.0.0.1"
    global ack_number
    ack_number = 0
    global logic_clock
    global net_q
    while True:
        time.sleep(sleepVal)
        data = conn.recv(1024)
        # print("msg received\n")
        dataVal = data.decode('ascii')
        dataVal = eval(dataVal)
        # dataVal = dataVal[1:-1].split(',')

        # 收到request
        if(dataVal[1]=="req"):
            net_q.append(dataVal)
            net_q = sorted(net_q, key=lambda x: int(x[2]), reverse=False)
            print(net_q)
            logic_clock = max(logic_clock, int(dataVal[2]) + 1)
            helpers.write_data(log, ["Receive request", str(time.time() - START_TIME), str(len(net_q)), str(logic_clock)])
            #发送ack
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host,int(dataVal[0])))
            msg = (PORT, "ack", str(logic_clock))
            s.send(str(msg).encode("ascii"))
            data = helpers.write_data(log, ["Send ack back", str(time.time() - START_TIME), str(len(net_q)), msg])

        # 收到ack
        elif(dataVal[1]=="ack"):
            logic_clock = max(logic_clock, int(dataVal[2]) + 1)
            ack_number+=1
            helpers.write_data(log, ["Receive ack", str(time.time() - START_TIME), str(len(net_q)), str(logic_clock)])

            #能否获取资源
            if (int(net_q[0][0]) == PORT):
                if (ack_number == 2):
                    helpers.write_data(log, ["Using", str(time.time() - START_TIME), str(len(net_q)),
                                             str(logic_clock)])
                    time.sleep(2)
                    ack_number = 0
                #发送release
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((host, int(dataVal[0])))
                msg = (PORT, "rel", str(logic_clock))
                net_q = net_q[1:]
                s.send(str(msg).encode("ascii"))
                data = helpers.write_data(log, ["Send release to both", str(time.time() - START_TIME), str(len(net_q)), msg])

        #收到release
        elif(dataVal[1]=="rel"):
            logic_clock = max(logic_clock, int(dataVal[2]) + 1)
            net_q=net_q[1:]
            helpers.write_data(log, ["Receive release", str(time.time() - START_TIME), str(len(net_q)), str(logic_clock)])
            if(int(net_q[0][0])==PORT):
                if(ack_number==2):
                    helpers.write_data(log, ["Using", str(time.time() - START_TIME), str(len(net_q)),
                                             str(logic_clock)])
                    time.sleep(2)
                    ack_number = 0
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((host, int(dataVal[0])))
                msg = (PORT, "rel", str(logic_clock))
                s.send(str(msg).encode("ascii"))
                net_q = net_q[1:]
                data = helpers.write_data(log, ["Send release to both", str(time.time() - START_TIME), str(len(net_q)), msg])


def producer(log, config,portVal1, portVal2):
    global net_q
    host= "127.0.0.1"
    port1 = int(portVal1)
    port2 = int(portVal2)
    s1 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s2 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sleepVal = 0.0
    global roll_10
    #sema acquire
    try:
        s1.connect((host,port1))
        s2.connect((host,port2))
        while True:
            # Block while queue is not empty
            while roll_10 == -1:
                continue
            # Send a message based on roll of 1-10
            send_roll(log, config, s1, s2)
            # Reset roll to -1 to wait until next empty queue
            roll_10 = -1

    except socket.error as e:
        print ("Error connecting producer: %s" % e)
 
# Main logic of program. config: [localHost, conPort, prodPort1, prodPort2]
def machine(config, portDict):
    # Initialize machine by passing consumer port
    conPort = config[1]
    log, clock_rate = helpers.init_log(conPort, portDict)

    # Queue of messages containing timestamp values
    global net_q
    net_q = list()
    global logic_clock
    logic_clock = 0
    # Random roll out of 10, -1 if queue is not empty
    global roll_10
    roll_10 = -1

    # Initialize server, consumers, and producers
    init_threads(log, config)

    # Run clock cycles
    global START_TIME 
    START_TIME = time.time()
    while True:
        for i in range(roll_10):
            # # Update the logical clock
            # logic_clock += 1
            # # Receive message
            # if not len(net_q)==0:
            #     msg_T = net_q[0]
            #     net_q = net_q[1:]
            #     logic_clock = max(logic_clock, int(msg_T[2])+1)
            #     helpers.write_data(log, ["Recv", str(time.time() - START_TIME), str(len(net_q)), str(logic_clock)])
            # # If queue is empty
            # else:
            #     roll_10 = random.randint(1,10)
            #     # Block until event is logged
            #     while roll_10 != -1:
            #         continue
            roll_10 = random.randint(1, 3)
                 # Block until event is logged
            while roll_10 != -1:
                continue
        # sleep for remainder of 1 second
        time.sleep(1.0 - ((time.time() - START_TIME) % 1.0))


#发送request
def send_request(log,config,s1,s2):
    global net_q
    global logic_clock
    msg = (config[1], "req", str(logic_clock)) #申请资源
    net_q.append(msg)
    net_q = sorted(net_q, key=lambda x : int(x[2]), reverse=False)
    print(net_q)
    data = helpers.write_data(log, ["Send request to both", str(time.time() - START_TIME), str(len(net_q)), msg])
    s1.send(str(msg).encode('ascii'))
    s2.send(str(msg).encode('ascii'))

    return data

if __name__ == '__main__':
    # Global ports for all processes
    port1 = 2056
    port2 = 3056
    port3 = 4056
    portDict = {'port1': port1, 'port2': port2, 'port3': port3}
    localHost= "127.0.0.1"

    config1=[localHost, port1, port2, port3]
    p1 = Process(target=machine, args=(config1, portDict))
    config2=[localHost, port2, port3, port1]
    p2 = Process(target=machine, args=(config2, portDict))
    config3=[localHost, port3, port1, port2]
    p3 = Process(target=machine, args=(config3, portDict))

    start_time = time.time()
    p1.start()
    p2.start()
    p3.start()

    while True:
        if time.time() - start_time > 10.0:
            p1.terminate()
            p2.terminate()
            p3.terminate()
            print("killed")
            break

    p1.join()
    p2.join()
    p3.join()
