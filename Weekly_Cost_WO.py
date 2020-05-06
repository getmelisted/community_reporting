#This is used to generate a CSV file containing the total cost per Work Order type for the previous week.
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
    
    with open(file, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quoting=csv.QUOTE_MINIMAL)
            
        csv_writer.writerow(['Client Name', 'Number of Audit Pin Placement WO', 'Cost for Audit Pin Placement WO', 'Number of Escalation Pin Placement WO', 
        'Cost for Escalation Pin Placement WO', 'Number of Find URL WO', 'Cost for Find URL WO', 'Number of New/Update Attempt WO', 
        'Cost for New/Update Attempt WO', 'Number of New/Update Audit WO', 'Cost for New/Update Audit WO', 'Number of New/Update Start WO', 
        'Cost for New/Update Start WO', 'Number of Pin Placement WO', 'Cost for Pin Placement WO', 'Number Review Response WO', 
        'Cost for Review Response WO'])
        
        for array in arrays:
            csv_writer.writerow([array[1], array[2][1], array[2][2], array[3][1], array[3][2], array[4][1], array[4][2], array[5][1], array[5][2], array[6][1], 
            array[6][2], array[7][1], array[7][2], array[8][1], array[8][2], array[9][1], array[9][2]])

        print("CSV Created!")
        
############################################################################################################
#Sends an email with the weekly Work Orders cost.
############################################################################################################
#pip install postmarker
def SendEmail(attachment, body):
    from postmarker.core import PostmarkClient
    
    strbody = "Please find attached the cost associated to Work Orders for the previous week.</br></br><table><tr><t2>Global Work Orders Cost</t2></tr><tr>"
    strbody += "<td>Work Order Type<td>Number of Work Orders<td>Total Cost per Type</tr>"
    
    totalnb = 0
    totalcost = 0
    
    for bod in body:
        strbody +='<td>' + bod[0] + ": <td>" + str(bod[1]) + "<td>" + str(bod[2]) + "</tr>"
        totalnb += bod[1]
        totalcost += bod[2]
    
    strbody += '<td>Grand Total: <td>' + str(totalnb) + '<td>' + str(totalcost) + '</tr>'
    strbody +='</table>'
    
    postmark = PostmarkClient(server_token=config.getpostmarktoken())
    postmark.emails.send(
                         From='mdegano@sweetiq.com',
                         To='renilda@sweetiq.com',
                         Cc='mdegano@sweetiq.com',
                         Subject='Weekly Work Orders cost',
                         HtmlBody= strbody,
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
                    " partner_compensation_rate/100 as 'calculated_dollar_pymt', grouped_tp.client_name, grouped_tp.client_id" +
                    " FROM (" +
                    " SELECT c.client_name, c.client_id, WO_THRP.user_id, WO.dir_id, WO_TT.wo_task, WO_TT.wo_tt_name, DATE(WO_THRP.updatedAt) as 'date_tp', count(*) AS 'total_wos'" +
                    " FROM selfser_woms.throughput WO_THRP" +
                    " INNER JOIN WO on WO.wo_id = WO_THRP.foreign_id" +
                    " inner join client c on c.client_id = wo.client_id" +
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
#Queries the Database for the billing information.
############################################################################################################
def getclients(start, end, wos):   

    from mysql.connector import Error
    clients = []
    tempcli = []

    for wo in wos:
        
        if wo[12] not in tempcli:
            tempcli.append(str(wo[12]))

    print("Connecting to MySQL")
    try:
        mySQLconnection = config.getwomsdbconnection()
            
        cursor = mySQLconnection.cursor()
        print(f'*** Start SQL Query! ***')
        
        sqlquery = ("Select client_id, client_name from client where client_id in (" + ','.join(tempcli) + ")")
        
        cursor.execute(sqlquery)

        res = cursor.fetchall()
    
        for result in res:
            clients.append(result)
        
        print(f'*** End SQL Query! ***')
    except Error as e :
        print ("Error while connecting to MySQL", e)
    finally:
        #closing database connection.
        if(mySQLconnection.is_connected()):
            mySQLconnection.close()
            print("MySQL connection is closed")
            
            
    return clients
    
############################################################################################################
#Calculates the total amount of Work Orders and the total $ amount spent on them according to the passed type.
#Returns an array containing the total number of Work Orders and their total dollar amount.
############################################################################################################
def getnbwo(workorders, wotype, client):

    filteredwos = 0
    total = []
    sum = 0
    
    for wo in workorders:
        if client == 0:
            if wo[3] == wotype and wo[7] != 0 and wo[7] != 8 and wo[7] != 62 and wo[7] != 63:
                filteredwos = filteredwos + wo[5]
                sum = sum + wo[10]
        else:
            if wo[3] == wotype and wo[7] != 0 and wo[7] != 8 and wo[7] != 62 and wo[7] != 63 and wo[12] == client:
                filteredwos = filteredwos + wo[5]
                sum = sum + wo[10]
                
    total = [wotype, filteredwos, sum]

    return total
    
############################################################################################################
#Execution starts here.
############################################################################################################
def main():

    from datetime import timedelta
    from datetime import date
    
    today = date.today()
    sunday = today - timedelta(days=(today.weekday() - 6) % 14)
    saturday = today - timedelta(days=(today.weekday() - 5) % 7)

    #saturday = '2019-12-31'
    #sunday = '2019-01-01'

    workorders = getWorkOrders(sunday, saturday)
    print("Total number of Work Orders: " + str(len(workorders)))

    auditpinwo = getnbwo(workorders, "Audit Pin Placement", 0)
    print(auditpinwo)
    
    escalationpinwo = getnbwo(workorders, "Escalation Pin Placement", 0)
    print(escalationpinwo)
    
    findurlwo = getnbwo(workorders, "Find URL", 0)
    print(findurlwo)
    
    newupdateattemptwo = getnbwo(workorders, "New/Update Attempt", 0)
    print(newupdateattemptwo)
    
    newupdateauditwo = getnbwo(workorders, "New/Update Audit", 0)
    print(newupdateauditwo)
    
    newupdatestartwo = getnbwo(workorders, "New/Update Start", 0)
    print(newupdatestartwo)
    
    pinplacementwo = getnbwo(workorders, "Pin Placement", 0)
    print(pinplacementwo)
    
    reviewresponsewo = getnbwo(workorders, "Review Response", 0)
    print(reviewresponsewo)
    
    clients = getclients(sunday, saturday, workorders)
    print(clients)
    
    clientstats = []
    
    for client in clients:
        tempauditpin = getnbwo(workorders, "Audit Pin Placement", client[0])
        tempescalationpin = getnbwo(workorders, "Escalation Pin Placement", client[0])
        tempfindurlwo = getnbwo(workorders, "Find URL", client[0])
        tempnewupdateattemptwo = getnbwo(workorders, "New/Update Attempt", client[0])
        tempnewupdateauditwo = getnbwo(workorders, "New/Update Audit", client[0])
        tempnewupdatestartwo = getnbwo(workorders, "New/Update Start", client[0])
        temppinplacementwo = getnbwo(workorders, "Pin Placement", client[0])
        tempreviewresponsewo = getnbwo(workorders, "Review Response", client[0])
        
        clientstats.append([client[0], client[1], tempauditpin, tempescalationpin, tempfindurlwo, tempnewupdateattemptwo, tempnewupdateauditwo, tempnewupdatestartwo, temppinplacementwo, tempreviewresponsewo])

    print(clientstats)
    
    file = "./Archive/WeeklyCostWos - " + str(sunday) + " to " + str(saturday) + ".csv"

    body = [auditpinwo, escalationpinwo, findurlwo, newupdateattemptwo, newupdateauditwo, newupdatestartwo, pinplacementwo, reviewresponsewo]
    writeCSV(clientstats, file)
    
    SendEmail(file, body)
if __name__ == '__main__': main()
