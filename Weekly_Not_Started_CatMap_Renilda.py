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
#Writes the Work Orders Cost in a CSV.
############################################################################################################
def writeCSV(arrays, file):

    createarchive('./Archive')
    deleteFileifExist(file)
    
    with open(file, 'w', newline='', encoding='cp1252') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quoting=csv.QUOTE_MINIMAL)
            
        csv_writer.writerow(['Work Order ID'])
        
        for array in arrays:
            csv_writer.writerow(["https://ss.swiq3.com/si_wodetail.asp?wo=" + str(array[0])])

        print("CSV Created!")
        
############################################################################################################
#Sends an email with the weekly Work Orders cost.
############################################################################################################
#pip install postmarker
def SendEmail(attachment):
    from postmarker.core import PostmarkClient
    
    strbody = ("Please find attached the list of all Category Mapping Work Orders that are in a Not Started status. " +
              "This list includes Work Orders for canceled locations and clients as active locations can share the same Category")

    
    postmark = PostmarkClient(server_token=config.getpostmarktoken())
    postmark.emails.send(
                         From='mdegano@sweetiq.com',
                         To='renilda@sweetiq.com',
                         Cc='mdegano@sweetiq.com',
                         Subject='Weekly Not Started Category Mapping Work Orders',
                         HtmlBody= strbody,
                         Attachments = [attachment]
                         )
    return
    
############################################################################################################
#Queries the Database.
############################################################################################################
def getWorkOrders():

    from mysql.connector import Error
    workorders = []
    
    print("Connecting to MySQL")
    try:
        mySQLconnection = config.getwomsdbconnection()
            
        cursor = mySQLconnection.cursor()
        print(f'*** Start SQL Query! ***')
        
        sqlquery = ("Select wo_id from wo where wo.wo_type = 7 and wo_status = 1")
        
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
#Execution starts here.
############################################################################################################
def main():

    workorders = getWorkOrders()
    file = "./Archive/NotStartedCatMapWO.csv"
    writeCSV(workorders, file)
    
    SendEmail(file)
if __name__ == '__main__': main()
