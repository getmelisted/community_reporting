/usr/local/bin/docker build -t weekly_community --build-arg MYSQL_WOMS_PROD=$MYSQL_WOMS_PROD --build-arg API_KEY_POSTMARK=$API_KEY_POSTMARK .
/usr/local/bin/docker run weekly_community python Weekly_Facebook_Community_Results.py
/usr/local/bin/docker run --rm weekly_community python Weekly_GMB_Community_Results.py