#This is used to generate a CSV file containing the total cost per Directory for the previous month.
import mysql.connector
import os
import os.path
import csv
import datetime
from config import *

############################################################################################################
#Deletes passed file if file already exists
############################################################################################################
def deleteFileifExist(filepath):

    if os.path.exists(filepath):
        os.remove(filepath)
        print("Previous file deleted")

############################################################################################################
#Creates the Archive folder if it does not exist
############################################################################################################
def createarchive(archive):

    if not os.path.exists(archive):
        os.makedirs(archive)

############################################################################################################
#Writes the Directory Cost in a CSV.
############################################################################################################
def writeCSV(dircost, file):
 
    createarchive('./Archive')
    deleteFileifExist(file)
    
    with open(file, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quoting=csv.QUOTE_MINIMAL)
            
        csv_writer.writerow(['Directory ID', 'Directory Name', 'Client ID', 'Client Name', 'Total number of Work Orders', 'Total Cost'])
        
        totalwos = 0
        totalcost = 0
        
        for dir in dircost:
            if dir[1] == "NULL":
                csv_writer.writerow([0, dir[1], dir[2], dir[3], dir[4], dir[5]])
            else:
                csv_writer.writerow([dir[0], dir[1], dir[2], dir[3], dir[4], dir[5]])
            totalwos = totalwos + dir[2]
            totalcost = totalcost + dir[5]
            
        csv_writer.writerow(["Grand total", "", "", "", totalwos, totalcost])

        print("CSV Created!")
        
############################################################################################################
#Sends an email with the weekly Work Orders cost.
############################################################################################################
#pip install postmarker
def SendEmail(attachment):
    from postmarker.core import PostmarkClient
    postmark = PostmarkClient(server_token=config.getpostmarktoken())
    postmark.emails.send(
                         From='mdegano@sweetiq.com',
                         To='cshapiro@sweetiq.com',
                         Cc='mdegano@sweetiq.com',
                         Subject='Monthly Directory cost per Client',
                         HtmlBody= 'Please find attached the cost associated to Directories per Clients for the previous month.',
                         Attachments = [attachment]
                         )
    return

############################################################################################################
#Queries the Database for the billing information.
############################################################################################################
def getWorkOrders(start, end):

    from mysql.connector import Error
    workorders = []
    
    print("Connecting to MySQL")
    try:
        mySQLconnection = config.getwomsdbconnection()
            
        cursor = mySQLconnection.cursor()
        print(f'*** Start SQL Query! ***')
        
        sqlquery = ("SELECT grouped_tp.user_id, grouped_tp.dir_id, grouped_tp.wo_task, grouped_tp.wo_tt_name, grouped_tp.date_tp, grouped_tp.total_wos, ifnull(DRI.adjusted_time, 0) as 'DRI'," + 
                    " tb_user.partner_id, tb_partner.partner_compensation_rate, total_wos * ifnull(DRI.adjusted_time, 0) as 'total dri', total_wos * ifnull(DRI.adjusted_time, 0) *" +
                    " partner_compensation_rate/100 as 'calculated_dollar_pymt', grouped_tp.client_id" +
                    " FROM (" +
                    " SELECT  WO_THRP.user_id, WO.dir_id, WO_TT.wo_task, WO_TT.wo_tt_name, DATE(WO_THRP.updatedAt) as 'date_tp', count(*) AS 'total_wos', WO.client_id" +
                    " FROM selfser_woms.throughput WO_THRP" +
                    " INNER JOIN WO on WO.wo_id = WO_THRP.foreign_id" +
                    " INNER JOIN (SELECT * FROM wo_type_transition WHERE wo_task IN (1, 2, 3, 4, 5, 9, 10, 11, 12) ) WO_TT ON FIND_IN_SET(WO.wo_type, WO_TT.wo_types) > 0" +
                    " AND FIND_IN_SET(WO_THRP.old_status , WO_TT.old_statuses) > 0 AND FIND_IN_SET(WO_THRP.new_status , WO_TT.new_statuses) > 0" +
                    " WHERE WO_THRP.user_id != 1 AND" +
                    " WO_THRP.updatedAt between '" + str(start) + " 00:00:00' and '" + str(end) + " 23:59:59'" +
                    " GROUP BY WO_THRP.user_id, WO.dir_id, WO_TT.wo_task, WO.client_id, DATE(WO_THRP.updatedAt)" +
                    " ) as grouped_tp" +
                    " INNER JOIN (select * FROM DRI WHERE date(createdAt) between '" + str(start) + "' and '" + str(end) + "') DRI ON dri.wo_task = grouped_tp.wo_task AND" +
                    " (DRI.dir_id <=> grouped_tp.dir_id ) AND DATE(DRI.createdAt) = grouped_tp.date_tp" +
                    " LEFT JOIN user tb_user ON tb_user.user_id = grouped_tp.user_id" +
                    " LEFT JOIN partner as tb_partner ON tb_partner.partner_id = tb_user.partner_id" +
                    " where tb_user.partner_id not in (0,8,62,63)")
        
        cursor.execute(sqlquery)

        res = cursor.fetchall()
    
        for result in res:
            workorders.append(result)
        
        print(f'*** End SQL Query! ***')
    except Error as e :
        print ("Error while connecting to MySQL", e)
    finally:
        #closing database connection.
        if(mySQLconnection.is_connected()):
            mySQLconnection.close()
            print("MySQL connection is closed")
            
    return workorders

############################################################################################################
#Calculates the total amount of Work Orders and the total $ amount spent on them according to the passed directory and client.
#Returns an array containing the total number of Work Orders and their total dollar amount.
############################################################################################################
def getnbwo(workorders, dirid, dirname, cliid, cliname):

    filteredwos = 0
    total = []
    sum = 0

    for wo in workorders:
        if wo[1] == dirid and wo[11] == cliid and wo[7] != 0 and wo[7] != 8 and wo[7] != 62 and wo[7] != 63:
            filteredwos = filteredwos + wo[5]
            sum = sum + wo[10]
            
    total = [dirid, dirname, cliid, cliname, filteredwos, sum]

    return total
    
############################################################################################################
#Extracts the different directories from the passed Work Orders.
############################################################################################################
def getdirectories(workorders):

    from mysql.connector import Error
    
    dirids = []
    directories = []
    
    print("Connecting to MySQL")
    try:
        mySQLconnection = config.getwomsdbconnection()
            
        cursor = mySQLconnection.cursor()
        print(f'*** Start SQL Query! ***')
    
        for wo in workorders:
            if wo[1] not in dirids and wo[7] != 0 and wo[7] != 8 and wo[7] != 16:
                if str(wo[1]) != "None": 
                    sqlquery = ("Select dir_name from directory where dir_status = 2 and dir_id = " + str(wo[1]))
                
                    cursor.execute(sqlquery)
                    res = cursor.fetchall()
    
                    for result in res:
                        dirname = result[0]
                        
                    dirid = wo[1]
                else:
                    dirname = "NULL"
                    dirid = wo[1]
                    
                dirids.append(dirid)
                directories.append([dirid, str(dirname)])
                print("New directory: " + str(dirid) + " " + str(dirname))
        
        print(f'*** End SQL Query! ***')
    except Error as e :
        print ("Error while connecting to MySQL", e)
    finally:
        #closing database connection.
        if(mySQLconnection.is_connected()):
            mySQLconnection.close()
            print("MySQL connection is closed")

    return directories
    
############################################################################################################
#Extracts the different clients from the passed Work Orders.
############################################################################################################
def getclients(workorders):

    from mysql.connector import Error
    
    cliids = []
    directories = []
    
    print("Connecting to MySQL")
    try:
        mySQLconnection = config.getwomsdbconnection()
            
        cursor = mySQLconnection.cursor()
        print(f'*** Start SQL Query! ***')
    
        for wo in workorders:
            if wo[11] not in cliids and wo[7] != 0 and wo[7] != 8 and wo[7] != 16:
                if str(wo[11]) != "None": 
                    sqlquery = ("Select client_name from client where client_status = 2 and client_id = " + str(wo[11]))
                
                    cursor.execute(sqlquery)
                    res = cursor.fetchall()
    
                    for result in res:
                        cliname = result[0]
                        
                    cliid = wo[11]
                else:
                    cliname = "NULL"
                    cliid = wo[11]
                    
                cliids.append(cliid)
                directories.append([cliid, str(cliname)])
                print("New client: " + str(cliid) + " " + str(cliname))
        
        print(f'*** End SQL Query! ***')
    except Error as e :
        print ("Error while connecting to MySQL", e)
    finally:
        #closing database connection.
        if(mySQLconnection.is_connected()):
            mySQLconnection.close()
            print("MySQL connection is closed")

    return directories

############################################################################################################
#Execution starts here.
############################################################################################################
def main():

    from datetime import timedelta
    from datetime import date
    
    today = date.today()

    first = today.replace(day=1)
    lastday = first - datetime.timedelta(days=1)
    firstday = lastday.replace(day=1)    

    #lastday = '2019-09-30'
    #firstday = '2019-01-01'
    
    workorders = getWorkOrders(firstday, lastday)
    print("Total number of Work Orders: " + str(len(workorders)))

    directories = getdirectories(workorders)
    clients = getclients(workorders)
    
    dircost = []
    
    for dir in directories:
        for cli in clients:
            dircost.append(getnbwo(workorders, dir[0], dir[1], cli[0], cli[1]))
    
    print(dircost)
    file = "./Archive/MonthlyCostDirectory - " + str(firstday) + " to " + str(lastday) + ".csv"

    writeCSV(dircost, file)
    
    SendEmail(file)
if __name__ == '__main__': main()
