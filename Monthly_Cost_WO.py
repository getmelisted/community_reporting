#This is used to generate a CSV file containing the total cost per Work Order type for the previous week.
import os
import os.path
import csv
import datetime
from urllib.parse import urlparse
import pymysql

############################################################################################################
#Returns the database cursor
############################################################################################################
def get_woms_cursor(connection_string):
    mysql_params = urlparse(connection_string)

    conn = pymysql.Connect(
        user=mysql_params.username,
        passwd=mysql_params.password,
        host=mysql_params.hostname,
        port=mysql_params.port,
        db=mysql_params.path[1:],
        charset='utf8',
    )
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    return cursor

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
    
    strbody = "Please find attached the cost associated to Work Orders for the previous month."
    
    postmark = PostmarkClient(server_token=os.environ['API_KEY_POSTMARK'])
    postmark.emails.send(
                         From='marco.degano@uberall.com',
                         To='madalina.cadariu@uberall.com',
                         Cc='marco.degano@uberall.com',
                         Subject='Monthly Work Orders cost',
                         HtmlBody= strbody,
                         Attachments = [attachment]
                         )
    return

############################################################################################################
#Queries the Database for the billing information.
############################################################################################################
def getWorkOrders(start, end):

    workorders = []
    
    sqlquery = ("SELECT grouped_tp.user_id, grouped_tp.dir_id, grouped_tp.wo_task, grouped_tp.wo_tt_name, grouped_tp.date_tp, grouped_tp.total_wos, ifnull(DRI.adjusted_time, 0) as 'DRI'," + 
                " tb_user.partner_id, tb_partner.partner_compensation_rate, total_wos * ifnull(DRI.adjusted_time, 0) as 'total dri', total_wos * ifnull(DRI.adjusted_time, 0) *" +
                " partner_compensation_rate/100 as 'calculated_dollar_pymt', grouped_tp.client_id" +
                " FROM (" +
                " SELECT wo.client_id, WO_THRP.user_id, WO.dir_id, WO_TT.wo_task, WO_TT.wo_tt_name, DATE(WO_THRP.updatedAt) as 'date_tp', count(*) AS 'total_wos'" +
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

    cursor = get_woms_cursor(os.getenv('MYSQL_WOMS_PROD'))
    cursor.execute(sqlquery)

    res = cursor.fetchall()

    for r in res:
        workorders.append([r['user_id'],r['dir_id'],r['wo_task'],r['wo_tt_name'],r['date_tp'],r['total_wos'],r['DRI'],r['partner_id'],
        r['partner_compensation_rate'],r['total dri'],r['calculated_dollar_pymt'],r['client_id']])
            
    return workorders

############################################################################################################
#Queries the Database for the billing information.
############################################################################################################
def getclients(start, end, wos):   

    clients = []
    tempcli = []

    for wo in wos:
        
        if wo[11] not in tempcli:
            tempcli.append(str(wo[11]))

    sqlquery = ("Select client_id, client_name from client where client_id in (" + ','.join(tempcli) + ")")
    cursor = get_woms_cursor(os.getenv('MYSQL_WOMS_PROD'))
    cursor.execute(sqlquery)

    res = cursor.fetchall()

    for r in res:
        clients.append([r['client_id'],r['client_name']])    
            
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
            if wo[3] == wotype and wo[7] != 0 and wo[7] != 8 and wo[7] != 62 and wo[7] != 63 and wo[11] == client:
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

    first = today.replace(day=1)
    lastday = first - datetime.timedelta(days=1)
    firstday = lastday.replace(day=1)
    
    #lastday = '2019-03-31'
    #firstday = '2019-01-01'

    workorders = getWorkOrders(firstday, lastday)
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
    
    clients = getclients(firstday, lastday, workorders)
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
    
    file = "./Archive/MonthlyCostWos - " + str(firstday) + " to " + str(lastday) + ".csv"

    body = [auditpinwo, escalationpinwo, findurlwo, newupdateattemptwo, newupdateauditwo, newupdatestartwo, pinplacementwo, reviewresponsewo]
    writeCSV(clientstats, file)
    
    SendEmail(file, body)

if __name__ == '__main__': main()
