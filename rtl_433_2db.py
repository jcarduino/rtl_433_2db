# Please create a mysql database for user rtl433db with create rights so table can be created
# change ip for database server
# install mysql connector
# install phython 2.7
# let it run ;)
#!/usr/bin/python
#import sys
import subprocess
import time
import threading
import Queue
import mysql.connector
from mysql.connector import errorcode

config = {
  'user': 'rtl433db',
  'password': 'fWMqwmFNKbK9upjT',
  'host': '192.168.0.8',
  'database': 'rtl433db',
  'raise_on_warnings': True,
}



class AsynchronousFileReader(threading.Thread):
    '''
    Helper class to implement asynchronous reading of a file
    in a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    '''

    def __init__(self, fd, queue):
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue

    def run(self):
        '''The body of the tread: read lines and put them on the queue.'''
        for line in iter(self._fd.readline, ''):
            self._queue.put(line)

    def eof(self):
        '''Check whether there is no more content to expect.'''
        return not self.is_alive() and self._queue.empty()

def replace(string):
    while '  ' in string:
        string = string.replace('  ', ' ')
    return string




def startsubprocess(command):
    '''
    Example of how to consume standard output and standard error of
    a subprocess asynchronously without risk on deadlocking.
    '''
    print "\n\nStarting sub process " + command + "\n"
    # Launch the command as subprocess.

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Launch the asynchronous readers of the process' stdout and stderr.
    stdout_queue = Queue.Queue()
    stdout_reader = AsynchronousFileReader(process.stdout, stdout_queue)
    stdout_reader.start()
    stderr_queue = Queue.Queue()
    stderr_reader = AsynchronousFileReader(process.stderr, stderr_queue)
    stderr_reader.start()
    # do database stuff init
    try:
        print("Connecting to database")
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exists, please create it before using this script.")
            print("Tables can be created by the script.")
        else:
            print(err)
    reconnectdb=0#if 0 then no error or need ro be reconnected
    #else:
    #cnx.close()
    cursor = cnx.cursor()
    TABLES = {}
    TABLES['SensorData'] = (
        "CREATE TABLE `SensorData` ("
        "  `sensor_id` INT UNSIGNED NOT NULL,"
        "  `whatdata` varchar(50) NOT NULL,"
        "  `data` float NOT NULL,"
        "  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        ") ENGINE =InnoDB DEFAULT CHARSET=latin1")
    for name, ddl in TABLES.iteritems():
        try:
            print("Checking table {}: ".format(name))
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Table seams to exist, no need to create it.")
            else:
                print(err.msg)
        else:
            print("OK")
    add_sensordata= ("INSERT INTO SensorData "
                     "(sensor_id, whatdata, data) "
                     "VALUES (%s, %s, %s)")

    # do queue loop, entering data to database
    # Check the queues if we received some output (until there is nothing more to get).
    while not stdout_reader.eof() or not stderr_reader.eof():
        # Show what we received from standard output.
        while not stdout_queue.empty():
            line = stdout_queue.get()
            print repr(line)

        # Show what we received from standard error.
        status=0 # waiting for event, 1= Rain, 2=wind, 3=Temperature, 4=NewKaku
        while not stderr_queue.empty():
            line = replace(stderr_queue.get())

            if ('Rain gauge event'in line):
                print '======== RAIN EVENT ========'
                status=1

            elif 'Wind event' in line:
                print "======== WIND EVENT ========"
                status=2
            elif 'Temperature event' in line:
                print "======== TEMP EVENT ========"
                status=3
            elif  'Sensor NewKaku event:' in line:
                print "======== KAKU EVENT ========"
                status=4
            else:
                if status==0:
                    print str(line) #Print stuff without processing
                if status==1:#rain
                    if 'Device'in line:
                        tmp,tmp=line.split('=')
                        device=tmp
                        print "Device " + device
                    if 'Rainfall ='in line:
                        tmp,tmp=line.split('=')
                        rainfall=float(tmp)
                        print "Rainfall " + str(rainfall)
                        #######################
                        #last field, put in db
                        # UPDATE DB
                        #########################
                        if reconnectdb:
                            print("Trying reconnecting to database")
                            cnx.reconnect()
                            reconnectdb=0
                        
                        try:
                            sensordata = (device,'Rainsensor',rainfall)
                            cursor.execute(add_sensordata,sensordata)
                            # Make sure data is committed to the database
                            cnx.commit()
                        except:
                            reconnectdb=1
                            print("Error connecting to database")
                if status==2:#wind
                    if 'Device'in line:
                        tmp,tmp=line.split('=')
                        device=tmp
                        print "Device " +device
                    if 'Wind speed'in line:
                        tmp,tmp,tmp,tmp,tmp,tmp,tmp,tmp1=line.split(' ')
                        wspeed=float(tmp)
                        print "Wspeed " + str(wspeed)
                    if 'Wind gust'in line:
                        tmp,tmp,tmp,tmp,tmp,tmp,tmp,tmp1=line.split(' ')
                        wgust=float(tmp)
                        print "Wgust " + str(wgust)
                    if 'Direction'in line:
                        tmp,tmp,tmp,tmp1=line.split(' ')
                        direction=int(tmp)
                        print "Direction " + str(direction)
                        #######################
                        #last field, put in db
                        # UPDATE DB
                        #########################
                        try:
                            if reconnectdb:
                                print("Trying reconnecting to database")
                                cnx.reconnect()
                                reconnectdb=0
                            sensordata = (device,'Windspeed',wspeed)
                            cursor.execute(add_sensordata,sensordata)
                            # Make sure data is committed to the database
                            #cnx.commit()
                            sensordata = (device,'Windgust',wgust)
                            cursor.execute(add_sensordata,sensordata)
                            sensordata = (device, 'Winddirection',direction)
                            # Make sure data is committed to the database
                            cnx.commit()
                        except:
                            print("Error connecting to database")
                            reconnectdb=1
                if status==3:#temp
                    if 'Device'in line:
                        tmp,tmp=line.split('=')
                        device=tmp
                        print "Device " + device
                    if 'Temp ='in line:
                        tmp,tmp=line.split('=')
                        temp=float(tmp)
                        print "Temp= " + str(temp)
                    if 'Humidity'in line:
                        tmp=1
                        tmp,tmp=line.split('=')
                        hum=int(tmp)
                        print "Humidity= " + str(hum)
                        #######################
                        #last field, put in db
                        # UPDATE DB
                        #########################
                        try:
                            if reconnectdb:
                                print("Trying reconnecting to database")
                                cnx.reconnect()
                                reconnectdb=0
                            sensordata = (device,'Temperature',temp)
                            cursor.execute(add_sensordata,sensordata)
                            # Make sure data is committed to the database
                            #cnx.commit()
                            sensordata = (device,'Humidity',hum)
                            cursor.execute(add_sensordata,sensordata)
                            # Make sure data is committed to the database
                            cnx.commit()
                        except:
                            print("Error connecting to database")
                            reconnectdb=1
                if status==4:#kaku
                    if 'KakuId'in line:
                        tmp,tmp,tmp,tmp1=line.split(' ')
                        device=tmp
                    if 'Unit'in line:
                        tmp,tmp,tmp,tmp1=line.split(' ')
                        unit=tmp
                    if 'Command'in line:
                        tmp,tmp=line.split('=')
                        command=tmp.replace(' ', '')
                        command=command.rstrip()
                    if 'Dim ='in line:
                        tmp,tmp=line.split('=')
                        dim=tmp.replace(' ', '')
                        dim=dim.rstrip()
                    if 'Group Call ='in line:
                        tmp,tmp=line.split('=')
                        group=tmp.rstrip()
                    if 'Dim Value ='in line:
                        tmp,tmp=line.split('=')
                        dimvalue=int(tmp.replace(' ', ''))
                        #######################
                        #last field, put in db
                        # UPDATE DB
                        #########################
                        try:
                            if reconnectdb:
                                print("Trying reconnecting to database")
                                cnx.reconnect()
                                reconnectdb=0
                            print("Kaku ID "+str(device)+" Unit "+unit+" Grp"+group+" Do "+command+" Dim "+dim)
                            sensordata = (device,'Kaku '+unit+' Grp'+group+" Do "+command+ ' Dim '+ dim,dimvalue)
                            cursor.execute(add_sensordata,sensordata)
                            # Make sure data is committed to the database
                            print("committing")
                            cnx.commit()
                        except mysql.connector.Error as err:
                            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                                print("Table seams to exist, no need to create it.")
                            else:
                                print(err.msg)
                            reconnectdb=1
                            print("Error connecting to database")
        # Sleep a bit before asking the readers again.
        time.sleep(.1)

    # Let's be tidy and join the threads we've started.
    try:
        cursor.close()
        cnx.close()
    except:
        pass
    stdout_reader.join()
    stderr_reader.join()

    # Close subprocess' file descriptors.
    process.stdout.close()
    process.stderr.close()


if __name__ == '__main__':
    # The main flow:
        #check if database is present, create tablesif no tables present


    startsubprocess("./rtl_433")
    print("Closing down")
