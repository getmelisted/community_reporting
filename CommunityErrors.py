import csv
import datetime
import mysql.connector
import os
import json
from config import *

############################################################################################################
#Writes the passed Array into a .csv file
############################################################################################################
def writeCSV(errors, file):
 
    createarchive('./Archive')
    deleteFileifExist(file)
    
    with open(file, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quoting=csv.QUOTE_MINIMAL)
            
        csv_writer.writerow(['Client Name', 'Client ID', 'BranchID', 'Account Manager', 'Location Name', 'Location Address', 'Work Order ID', 'Error Message', 'Directory', 'Location ID', 'Directory ID', 'Error Date'])
        
        for error in errors:
             csv_writer.writerow([error[0],error[1],error[2],error[3],error[4],error[5],error[6], str(error[7]).replace('<font color="green">', ''),error[8],error[10],error[11],error[12]])

        print("CSV Created!")
        
############################################################################################################
#Searches for a file and deletes it.
############################################################################################################
def deleteFileifExist(filepath):
    # As file at filePath is deleted now, so we should check if file exists or not not before deleting them
    if os.path.exists(filepath):
        os.remove(filepath)
        print("Previous report deleted")
    else:
        print("Cannot delete the file as it doesn't exist")
        
############################################################################################################
#Creates the Archive folder if it does not exist
############################################################################################################
def createarchive(archive):

    if not os.path.exists(archive):
        os.makedirs(archive)

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
                         Subject='Community API Errors',
                         HtmlBody= 'Please find attached the Work Orders in API Errors after the Community Audit.',
                         Attachments = [attachment]
                         )
    return
    
############################################################################################################
#fetches all the API errors from WOMS DB
############################################################################################################
def getAPIErrors(SQLfile):
    from mysql.connector import Error
    
    # Open and read the file as a single buffer
    fd = open(SQLfile, 'r')
    SQLQuery = fd.read()
    fd.close()
    
    apierrors = []
    
    print("Fetching API Errors...")
    try:
        mySQLconnection = config.getwomsdbconnection()
            
        cursor = mySQLconnection.cursor()
        cursor.execute(SQLQuery)
        
        apierrors = cursor.fetchall()
        
    except Error as e :
        print ("Error while connecting to MySQL", e)
    finally:
        #closing database connection.
        if(mySQLconnection.is_connected()):
            mySQLconnection.close()
            print("MySQL connection is closed")
        
    return apierrors

############################################################################################################
#Seperates the Community failed Work Orders from the true API errors
############################################################################################################
def filtercommunity(fullerrors):
    
    community = []
    api = []
    
    for error in fullerrors:
        
        if 'font color="green"' in error[7]:
            community.append(error)
        else:
            api.append(error)
            
    return [community, api]

############################################################################################################
#Execution starts here.
############################################################################################################
def main():

    from datetime import timedelta
    from datetime import date
    
    today = date.today()
    
    SQLfile = 'APIErrors.sql'
    errors = getAPIErrors(SQLfile)

    if len(errors) > 0:
        print(len(errors))
        filtered = filtercommunity(errors)
        
        communityerrors = filtered[0]

        print(str(len(communityerrors)) + " Community API Errors")
        
        writeCSV(communityerrors, 'Archive/Community API Errors - ' + str(today) + '.csv')
        SendEmail('Archive/Community API Errors - ' + str(today) + '.csv')
        
    else:
        print("No errors returned!")
    
if __name__ == '__main__': main()