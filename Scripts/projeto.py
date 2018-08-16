#! /home/rance/anaconda3/bin/python

# this script is intended to be run once a day, you could automate it with a software like cron

import os
from ftplib import FTP
import psycopg2
from psycopg2 import sql
from datetime import date, timedelta

# replaces North and South for + and minus to make it easy to calculate distance
def latitude_calculator(latitude):
    if latitude[0] == 'N':
        n_latitude = int(latitude[1:])
    else:
        n_latitude = -1 * int(latitude[1:])
    return n_latitude

# generic method that connects to ftp server and downloads data
def updateReport(path_in_server, local_path, file):
    ftp = FTP('ftp.swpc.noaa.gov')
    ftp.login()

    ftp.cwd(path_in_server)

    localfile = open(local_path + file, 'wb')
    ftp.retrbinary('RETR ' + file, localfile.write, 1024)
    localfile.close()
    ftp.quit()
    return

# analyzes the downloaded file and puts relevant data in the database
def parseEventReport(local_path, file, file_prefix):
    localfile = open(local_path + file, 'r')
    file_in_lines = localfile.read().split('\n')
    relevant_data = file_in_lines[13:]
    for line in relevant_data:
        fields = list(filter(lambda x: x != '+', filter(None, line.split(' '))))
        if  len(fields) == 11 and fields[6] == 'XRA':
            cur.execute("INSERT INTO event_report (date_, event, begin_, max, end_, obs, q, type, loc_frq, particular1, particular2, reg) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (file_prefix, fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9], fields[10],))
    return

# analyzes the download file and puts data in the database,
# also inspect if it is an area that is returning and links
# to it if it is, mainstains a database table of 'due to return' areas
def parseRegionReport(local_path, file):
    localfile = open(local_path + file, 'r')
    file_in_lines = localfile.read().split('\n')
    relevant_data = file_in_lines[10:]
    index = 0
    cur.execute("SELECT * FROM due_to_return as d")
    due2return = cur.fetchall()
    for line in relevant_data:
        fields = list(filter(None, line.split(' ')))
        if len(fields) == 8:
            n_latitude = latitude_calculator(fields[1][:3])
            has_previous=0
            for item in due2return:
                t_latitude = latitude_calculator(item[1])
                if n_latitude <= (t_latitude+3) and n_latitude >= (t_latitude-3):
                    has_previous = 1
                    cur.execute("INSERT INTO region_report (prev_nmbr, nmbr, location, lo, area, z, ll, nn, mag_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (item[0], fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7],))
            if has_previous == 0:
                cur.execute("INSERT INTO region_report (nmbr, location, lo, area, z, ll, nn, mag_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7],))
        elif fields[0] == 'IA.':
            relevant_data2 = relevant_data[index+2:]
            index = 0
            for line2 in relevant_data2:
                fields2 = list(filter(None, line2.split(' ')))
                if len(fields2) == 3:
                    n_latitude = latitude_calculator(fields2[1][:3])
                    has_previous = 0
                    for item in due2return:
                        t_latitude = latitude_calculator(item[1])
                        if n_latitude <= (t_latitude+3) and n_latitude >= (t_latitude-3):
                            has_previous = 1
                            cur.execute("INSERT INTO region_report (prev_nmbr, nmbr, location, lo) VALUES (%s, %s, %s, %s)", (item[0], fields2[0], fields2[1], fields2[2],))
                    if has_previous == 0:
                        cur.execute("INSERT INTO region_report (nmbr, location, lo) VALUES (%s, %s, %s)", (fields2[0], fields2[1], fields2[2],))
                elif fields2[0] == 'II.':
                    cur.execute("DELETE FROM public.due_to_return")
                    relevant_data3 = relevant_data2[index+2:]
                    for line3 in relevant_data3:
                        fields3 = list(filter(None, line3.split(' ')))
                        if len(fields3) == 3:
                            cur.execute("INSERT INTO due_to_return (nmbr, lat, lo) VALUES (%s, %s, %s)", (fields3[0], fields3[1], fields3[2],))
                    return
                index+=1
        index+=1

# downloads event report file, parse its contents and puts it in the database
def updateEventReport(file_prefix):
    path_in_server = 'pub/warehouse/2018/2018_events'
    file = file_prefix + "events.txt"
    local_path = '/home/rance/Documents/Materias/PAC/Projeto/Data/Reports/Event/'
    updateReport(path_in_server, local_path, file)
    parseEventReport(local_path , file, file_prefix)
    return

# downloads region report (SRS) file, parse its contents
# and puts it in the database, linking to previous rotations
# of the same area
def updateRegionReport(file_prefix):
    path_in_server = '/pub/warehouse/2018/SRS'
    file = file_prefix + "SRS.txt"
    local_path =  '/home/rance/Documents/Materias/PAC/Projeto/Data/Reports/ActiveRegion/'
    updateReport(path_in_server, local_path, file)
    parseRegionReport(local_path, file)
    return

# put the data in the warehouse
def sync(file_prefix):
    cur.execute("INSERT INTO warehouse (date_, event, begin_, max, end_, obs, q, type, loc_frq, particular1, particular2, prev_nmbr, nmbr, location, lo, area, z, ll, nn, mag_type) SELECT e.date_, e.event, e.begin_, e.max, e.end_, e.obs, e.q, e.type, e.loc_frq, e.particular1, e.particular2, r.prev_nmbr, r.nmbr, r.location, r.lo, r.area, r.z, r.ll, r.nn, r.mag_type FROM event_report as e RIGHT OUTER JOIN region_report as r ON e.reg = r.nmbr;")
    cur.execute("DELETE FROM public.event_report")
    cur.execute("DELETE FROM public.region_report")
    return

# we do this to get data from seven days before, having a secure margin as probably there is yet no data from the last few days
seven_days_before = date.today() - timedelta(days = 7)


conn = psycopg2.connect(dbname="postgres", user="postgres", password="password", host='localhost', port='5432')
cur = conn.cursor()

# the prefix of the files to be downloaded
file_prefix = seven_days_before.strftime("%Y%m%d")

updateEventReport(file_prefix)
updateRegionReport(file_prefix)
sync(file_prefix)

conn.commit()

cur.close()
conn.close()
