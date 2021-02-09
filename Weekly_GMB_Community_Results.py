#This is used to generate a CSV file containing the total cost per Work Order type for the previous month.
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
#Writes the insights data in a CSV.
############################################################################################################
def writeCSV(arrays, file):
 
    createarchive('./Archive')
    deleteFileifExist(file)
    
    with open(file, 'w', newline='', encoding='ISO-8859-1') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quoting=csv.QUOTE_MINIMAL)
        
        for array in arrays:
            csv_writer.writerow([array[0], array[1]])

        print("CSV Created!")
        
############################################################################################################
#Writes the RAW Work Orders in a CSV.
############################################################################################################
def writeRAWCSV(workorder, file):
 
    deleteFileifExist(file)
    
    with open(file, 'w', newline='', encoding='ISO-8859-1') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quoting=csv.QUOTE_MINIMAL)
        
        csv_writer.writerow(["wo_id", "wo_date", "wo_type", "elapsed_time", "updatedat", "user_name", "user_trust_level", "old_status", "new_status", "client id"])
        
        for wo in workorder:
            csv_writer.writerow([wo[0], wo[1], wo[2], wo[3], wo[4], wo[5], wo[6], wo[7], wo[8], wo[9]])

        print("CSV Created!") 
        
############################################################################################################
#Sends an email with the weekly Work Orders cost.
############################################################################################################
#pip install postmarker
def SendEmail(attachments):
    from postmarker.core import PostmarkClient
    postmark = PostmarkClient(server_token=config.getpostmarktoken())
    postmark.emails.send(
                         From='marco.degano@uberall.com',
                         To='madalina.cadariu@uberall.com',
                         Cc='marco.degano@uberall.com',
                         Subject='Weekly GMB Community Results',
                         HtmlBody= 'Please find attached the GMB Community Results associated to Work Orders for the previous week.',
                         Attachments = [attachments[0], attachments[1]]
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
        
        sqlquery = ("SELECT wo.wo_id, wo.wo_date,wo.wo_type,throughput.elapsed_time, throughput.updatedat, user.user_name, user.user_trust_level, throughput.old_status, throughput.new_status, wo.client_id" +
                    " from throughput" +
                    " inner join wo on wo.wo_id = throughput.foreign_id" +
                    " inner join user on user.user_id = throughput.user_id" +
                    " where wo.dir_id = 27" +
                    " and date (throughput.updatedat) >= '" + str(start) + "'" +
                    " and date (throughput.updatedat) <= '" + str(end) + "'" +
                    " and wo.wo_expireddate is null" +
                    " and user.partner_id not in (0,8,62,63)" +
                    " and wo.wo_type in (2,3)" +
                    " and throughput.new_status not in (13,12)")
        
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
#Get the number of Work Orders set to specified status
############################################################################################################
def getnewstatnb(workorders, newstat):

    total = 0
    
    for wo in workorders:
        if wo[8] == newstat:
            total = total + 1
    
    return total
    
############################################################################################################
#Get the percentage of Work Orders for the criteria.
############################################################################################################
def getpercentage(nbworkorders, nbwotype):

    if nbworkorders > 0:
        percentage = str(float(nbwotype)/float(nbworkorders) * 100) + "%"
        
    else:
        percentage = "No Work Orders this week"
    
    return percentage
    
############################################################################################################
#Get the percentage of Work Orders by type of workorder.
############################################################################################################
def getpercentagebytype(workorders, wotype, newstat, totalnewstat):

    total = 0
    if totalnewstat > 0:
        for wo in workorders:
            if wo[2] == wotype and wo[8] == newstat:
                total = total + 1

        percentage = str(float(total)/float(totalnewstat) * 100) + "%"
    
    else:
        percentage = "No Work Orders this week"
        
    return percentage
    
############################################################################################################
#Get the percentage of Work Orders by type of overall workorder.
############################################################################################################
def getpercentagebyoverallwotype(workorders, wotype, newstat):

    total = 0
    totaltype = 0

    for wo in workorders:
        if wo[2] == wotype:
            totaltype = totaltype + 1
            
            if wo[8] == newstat:
                total = total + 1
    if totaltype > 0:
        percentage = str(float(total)/float(totaltype) * 100) + "%"
    else:
        percentage = "No Work Orders this week"
        
    return percentage
    
############################################################################################################
#Returns the average from the passed array.
############################################################################################################
def getaverage(numbers):

    total = 0

    for num in numbers:
        total = total + int(num.days)

    if len(numbers) > 0:
        average = total/len(numbers)
    else:
        average = "No Work Orders this week"

    return average
    
############################################################################################################
#Execution starts here.
############################################################################################################
def main():

    from datetime import timedelta
    from datetime import date
    
    today = date.today()

    if today.weekday() < 6:
        sunday = today - timedelta(days=(today.weekday() - 6) % 14)
        saturday = today - timedelta(days=(today.weekday() - 5) % 7)
    else:
        sunday = today - timedelta(days=7)
        saturday = today - timedelta(days=1)

    workorders = getWorkOrders(sunday, saturday)
    #print(workorders)
    print("Total number of Work Orders: " + str(len(workorders)))

    totalwoattempt = ["Total Work Orders attempted", len(workorders)]
    wotoattempt2 = ["Work Orders to Attempt 2", getnewstatnb(workorders, 7)]
    wotopubaudit = ["Work Orders to Published Needs Auditing", getnewstatnb(workorders, 10)]
    wotosubpending = ["Work Orders to Submitted & Pending", getnewstatnb(workorders, 6)]
    wotofailed = ["Work Orders to Attempts Failed", getnewstatnb(workorders, 9)]
    wotormi = ["Work Orders to RMI", getnewstatnb(workorders, 3)]
    
    print(wotoattempt2)
    print(wotopubaudit)
    print(wotosubpending)
    print(wotofailed)
    print(wotormi)
    print("##############################################################")
    
    pertoattempt2 = ["% to Attempt 2", getpercentage(totalwoattempt[1], wotoattempt2[1])]
    pertopubaudit = ["% to Published Needs Auditing", getpercentage(totalwoattempt[1], wotopubaudit[1])]
    pertosubpending = ["% to Submitted & Pending", getpercentage(totalwoattempt[1], wotosubpending[1])]
    pertofailed = ["% to Attempts Failed", getpercentage(totalwoattempt[1], wotofailed[1])]
    pertormi = ["% to RMI", getpercentage(totalwoattempt[1], wotormi[1])]
    
    print(pertoattempt2)
    print(pertopubaudit)
    print(pertosubpending)
    print(pertofailed)
    print(pertormi)
    
    pertopubauditupdate = ["% Work Orders set to Published Needs Auditing that are Update WO", getpercentagebytype(workorders, 3, 10, wotopubaudit[1])]
    perupdatepubaudit = ["% Update Work Orders set to Published Needs Auditing", getpercentagebyoverallwotype(workorders, 3, 10)]
    print(pertopubauditupdate)
    print(perupdatepubaudit)
    
    ave = []
    for wo in workorders:
        ave.append(wo[4] - wo[1])
        
    averageage = ["Average Age of Work Orders (days)", getaverage(ave)]
    print(averageage)

    file = "./Archive/WeeklyGMBCommunityResults - " + str(sunday) + " to " + str(saturday) + ".csv"

    arrays =[totalwoattempt, wotoattempt2, wotopubaudit, wotosubpending, wotofailed, wotormi, pertoattempt2, pertopubaudit, pertosubpending, pertofailed, pertormi, pertopubauditupdate, perupdatepubaudit, averageage]
    writeCSV(arrays, file)
    
    file2 = "./Archive/RAW_WeeklyGMBCommunityResults - " + str(sunday) + " to " + str(saturday) + ".csv"
    writeRAWCSV(workorders, file2)
    
    files = [file, file2]
    
    SendEmail(files)
if __name__ == '__main__': main()
