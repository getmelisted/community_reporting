#This is used to generate a CSV file containing the total cost per Work Order type for the previous month.
import mysql.connector
import os
import os.path
import csv
import datetime
import gzip
from config import *

############################################################################################################
#Compresses file before attaching to email
############################################################################################################
def zip(filename):
    from zipfile import ZipFile
    
    deleteFileifExist(filename + '.zip')
    
    with ZipFile(filename + '.zip', 'w') as myzip:
        myzip.write(filename)

    return filename + '.zip'

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
#Writes the insights data in a CSV.
############################################################################################################
def writeCSV(arrays, file):
 
    createarchive('./Archive')
    deleteFileifExist(file)
    
    with open(file, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quoting=csv.QUOTE_MINIMAL)
        
        csv_writer.writerow(["User", "New", "Update", "Audit", "Pin Placement", "Audit Pin Placement", "Escalation Pin Placement", "Find URL", "Review Response"])
        
        for array in arrays:
            csv_writer.writerow([array[0], array[1], array[2], array[3], array[4], array[5], array[6], array[7], array[8]])

        print("CSV Created!")
        
############################################################################################################
#Sends an email with the weekly Work Orders cost.
############################################################################################################
#pip install postmarker
def SendEmail(attachment, arrays):
    from postmarker.core import PostmarkClient
    postmark = PostmarkClient(server_token=config.getpostmarktoken())
    body  = 'Please find below the Average Time to Complete per Work Order type for Work Orders of the previous week. A file with the average time to complete per user is attached.</br></br><table><tr><t2>Average Time to Complete in seconds</t2></tr>'
    body = body + '<tr>'
    for arr in arrays:
        body = body + '<td>' + arr[0] + ": <td>" + str(arr[1]) + "</tr>"
    
    body = body + '</table>'
    postmark.emails.send(
                         From='mdegano@sweetiq.com',
                         To='renilda@sweetiq.com',
                         Cc='mdegano@sweetiq.com',
                         Subject='Weekly Time to Complete',
                         HtmlBody= body,
                         Attachments=[attachment]
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
        
        sqlquery = ("SELECT wo.wo_id, wo.wo_date,wo.wo_type,throughput.elapsed_time, throughput.updatedat, user.user_name, user.user_trust_level, throughput.old_status, throughput.new_status" +
                    " from throughput" +
                    " inner join wo on wo.wo_id = throughput.foreign_id" +
                    " inner join user on user.user_id = throughput.user_id" +
                    " and date (throughput.updatedat) >= '" + str(start) + "'" +
                    " and date (throughput.updatedat) <= '" + str(end) + "'" +
                    " and wo.wo_expireddate is null" +
                    " and user.partner_id not in (0,8,62,63)" +
                    " and wo.wo_type in (2,3,5,6,9)")
        
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
#Gets the Work Orders from the raw data for the passed type.
#Type == -1 -> Audit new and update Work Orders.
#Type == 2 -> New Work Orders
#Type == 3 -> Update Work Orders
#Type == 9 -> Pin Placement Work Orders
#Type == 5 -> Fund URL Work Orders
#Type == 6 -> Review Response Work ORders
#Old_status == -1 -> Ignore Audit status (10)
#Old_status == 0 -> Ignore Old Status, take all
#Old-Status > 0 -> Only Work Orders with that old status
############################################################################################################
def getWorkOrdersbytype(workorders, type, old_status):

    wos = []
    
    for wo in workorders:
        if type == -1:
            if (wo[2] == 2 or wo[2] == 3) and wo[7] == 10:
                wos.append(wo)
        else:
            if wo[2] == type:
                if old_status == -1 and wo[7] != 10:
                    wos.append(wo)
                elif old_status == 0:
                    wos.append(wo)
                elif old_status == -2 and wo[7] != 10 and wo[7] != 7:
                    wos.append(wo)
                elif old_status > 0 and wo[7] == old_status:
                    wos.append(wo)

    return wos
    
############################################################################################################
#Queries the Database for the billing information.
############################################################################################################
def getusers(start, end):   

    from mysql.connector import Error
    users = []
    
    print("Connecting to MySQL")
    try:
        mySQLconnection = config.getwomsdbconnection()
            
        cursor = mySQLconnection.cursor()
        print(f'*** Start SQL Query! ***')
        
        sqlquery = ("SELECT user.user_name" +
                    " from throughput" +
                    " inner join wo on wo.wo_id = throughput.foreign_id" +
                    " inner join user on user.user_id = throughput.user_id" +
                    " and date (throughput.updatedat) >= '" + str(start) + "'" +
                    " and date (throughput.updatedat) <= '" + str(end) + "'" +
                    " and wo.wo_expireddate is null" +
                    " and user.partner_id not in (0,8,62,63)" +
                    " and wo.wo_type in (2,3,5,6,9)" +
                    " group by user.user_name")
        
        cursor.execute(sqlquery)

        res = cursor.fetchall()
    
        for result in res:
            users.append(result[0])
        
        print(f'*** End SQL Query! ***')
    except Error as e :
        print ("Error while connecting to MySQL", e)
    finally:
        #closing database connection.
        if(mySQLconnection.is_connected()):
            mySQLconnection.close()
            print("MySQL connection is closed")
            
    return users
    
############################################################################################################
#Returns the average from the passed array.
############################################################################################################
def getaverage(wos):

    from datetime import timedelta
    from datetime import date
    
    total = 0

    for wo in wos:
        total = total + wo[3]

    if len(wos) > 0:
        average = (total/len(wos))/1000
    else:
        average = "No Work Orders this week"
    
    return average

############################################################################################################
#Returns the average Completion time for each user.
############################################################################################################
def getavgusers(workorders, users):

    avguser = []
    
    for user in users:
        usr = []
        for wo in workorders:
            if wo[5] == user:
                usr.append(wo)
                
        newwo = getWorkOrdersbytype(usr, 2, -1)
        updatewo = getWorkOrdersbytype(usr, 3, -1)
        auditwo = getWorkOrdersbytype(usr, -1, 10)
        pinwo = getWorkOrdersbytype(usr, 9, -2)
        auditpin = getWorkOrdersbytype(usr, 9, 10)
        escalationpin = getWorkOrdersbytype(usr, 9, 7)
        findwo = getWorkOrdersbytype(usr, 5, 0)
        reviewresponsewo = getWorkOrdersbytype(usr, 6, 0)
        
        avgnew = getaverage(newwo)
        avgupdate = getaverage(updatewo)
        avgaudit = getaverage(auditwo)
        avgpin = getaverage(pinwo)
        avgauditpin = getaverage(auditpin)
        avgescpin = getaverage(escalationpin)
        avgfind = getaverage(findwo)
        avgreview = getaverage(reviewresponsewo)
        
        avguser.append([user, avgnew, avgupdate, avgaudit, avgpin, avgauditpin, avgescpin, avgfind, avgreview])
    
    return avguser
    
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
    #print(workorders)
    print("Total number of Work Orders: " + str(len(workorders)))

    newwo = getWorkOrdersbytype(workorders, 2, -1)
    updatewo = getWorkOrdersbytype(workorders, 3, -1)
    auditwo = getWorkOrdersbytype(workorders, -1, 10)
    pinwo = getWorkOrdersbytype(workorders, 9, -2)
    auditpin = getWorkOrdersbytype(workorders, 9, 10)
    escalationpin = getWorkOrdersbytype(workorders, 9, 7)
    findwo = getWorkOrdersbytype(workorders, 5, 0)
    reviewresponsewo = getWorkOrdersbytype(workorders, 6, 0)
    
    avgnew = ["New Work Orders", getaverage(newwo)]
    avgupdate = ["Update Work Orders", getaverage(updatewo)]
    avgaudit = ["Audit Work Orders", getaverage(auditwo)]
    avgpin = ["Pin Placement Work Orders", getaverage(pinwo)]
    avgauditpin = ["Pin Placement Audit Work Orders", getaverage(auditpin)]
    avgescpin = ["Pin Placement Escalation Work Orders", getaverage(escalationpin)]
    avgfind = ["Find URL Work Orders", getaverage(findwo)]
    avgreview = ["Review Response Work Orders", getaverage(reviewresponsewo)]
    
    users = getusers(sunday, saturday)
    print(users)
    print("Total number of Users: " + str(len(users)))
    avgperuser = getavgusers(workorders, users)
    
    file = "./Archive/WeeklyTimetoComplete - " + str(sunday) + " to " + str(saturday) + ".csv"

    writeCSV(avgperuser, file)
    
    arrays = [avgnew, avgupdate, avgaudit, avgpin, avgauditpin, avgescpin, avgfind, avgreview]
    SendEmail(file, arrays)
if __name__ == '__main__': main()
