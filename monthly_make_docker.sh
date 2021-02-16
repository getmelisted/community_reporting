docker build -t monthly_community --build-arg MYSQL_WOMS_PROD=$MYSQL_WOMS_PROD --build-arg API_KEY_POSTMARK=$API_KEY_POSTMARK .
docker run monthly_community python Monthly_Cost_WO.py