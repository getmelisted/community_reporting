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
    
    with open(file, 'w', newline='', encoding='ISO-8859-1') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quoting=csv.QUOTE_MINIMAL)
            
        csv_writer.writerow(['Directory ID', 'Directory Name', 'Total number of Work Orders', 'Total Cost'])
        
        totalwos = 0
        totalcost = 0
        
        for dir in dircost:
            if dir[1] == "NULL":
                csv_writer.writerow([0, dir[1], dir[2], dir[3]])
            else:
                csv_writer.writerow([dir[0], dir[1], dir[2], dir[3]])
            totalwos = totalwos + dir[2]
            totalcost = totalcost + dir[3]
            
        csv_writer.writerow(["Grand total", "", totalwos, totalcost])

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
                         To='renilda@sweetiq.com',
                         Cc='mdegano@sweetiq.com',
                         Subject='Weekly Directory cost',
                         HtmlBody= 'Please find attached the cost associated to Directories for the previous week.',
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
                    " partner_compensation_rate/100 as 'calculated_dollar_pymt'" +
                    " FROM (" +
                    " SELECT  WO_THRP.user_id, WO.dir_id, WO_TT.wo_task, WO_TT.wo_tt_name, DATE(WO_THRP.updatedAt) as 'date_tp', count(*) AS 'total_wos'" +
                    " FROM selfser_woms.throughput WO_THRP" +
                    " INNER JOIN WO on WO.wo_id = WO_THRP.foreign_id" +
                    " INNER JOIN (SELECT * FROM wo_type_transition WHERE wo_task IN (1, 2, 3, 4, 5, 9, 10, 11, 12) ) WO_TT ON FIND_IN_SET(WO.wo_type, WO_TT.wo_types) > 0" +
                    " AND FIND_IN_SET(WO_THRP.old_status , WO_TT.old_statuses) > 0 AND FIND_IN_SET(WO_THRP.new_status , WO_TT.new_statuses) > 0" +
                    " WHERE WO_THRP.user_id != 1 AND" +
                    " WO_THRP.updatedAt between '" + str(start) + " 00:00:00' and '" + str(end) + " 23:59:59'" +
                    " GROUP BY WO_THRP.user_id, WO.dir_id, WO_TT.wo_task, DATE(WO_THRP.updatedAt)" +
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
#Calculates the total amount of Work Orders and the total $ amount spent on them according to the passed directory.
#Returns an array containing the total number of Work Orders and their total dollar amount.
############################################################################################################
def getnbwo(workorders, dirid, dirname):

    filteredwos = []
    total = []
    sum = 0

    for wo in workorders:
        if wo[1] == dirid and wo[7] != 0 and wo[7] != 8 and wo[7] != 62 and wo[8] != 63:
            filteredwos.append(wo[10])
            sum = sum + wo[10]
            
    total = [dirid, dirname, len(filteredwos), sum]

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
            if wo[1] not in dirids and wo[7] != 0 and wo[7] != 8:
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
#Execution starts here.
############################################################################################################
def main():

    from datetime import timedelta
    from datetime import date

    
    today = date.today()

    sunday = today - timedelta(days=(today.weekday() - 6) % 14)
    saturday = today - timedelta(days=(today.weekday() - 5) % 7)  

    workorders = getWorkOrders(sunday, saturday)
    print("Total number of Work Orders: " + str(len(workorders)))

    directories = getdirectories(workorders)
    
    dircost = []
    
    for dir in directories:
        dircost.append(getnbwo(workorders, dir[0], dir[1]))
    
    print(dircost)
    file = "./Archive/MonthlyCostDirectory - " + str(sunday) + " to " + str(saturday) + ".csv"

    writeCSV(dircost, file)
    
    SendEmail(file)
if __name__ == '__main__': main()
