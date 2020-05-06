import mysql.connector
import os
from urllib.parse import urlparse

class config():
        
    def getwomsdbconnection():
        
        mysql_params = urlparse(os.environ['MYSQL_WOMS_PROD'])

        mySQLconnection = mysql.connector.connect(user=mysql_params.username,
                                                    passwd=mysql_params.password,
                                                    host=mysql_params.hostname,
                                                    port=mysql_params.port,
                                                    db=mysql_params.path[1:],
                                                    charset='utf8')
        
        return mySQLconnection
        
    def getpostmarktoken():
        
        postmarktoken = os.environ['API_KEY_POSTMARK']
        
        return postmarktoken