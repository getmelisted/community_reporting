FROM python:3.8

# Copy requirements.txt file
COPY requirements.txt .

# Install requirements.txt dependencies
RUN pip install -r requirements.txt

# To get local env
ENV TZ=Canada/Eastern
ARG MYSQL_WOMS_PROD
ARG API_KEY_POSTMARK

ENV MYSQL_WOMS_PROD=$MYSQL_WOMS_PROD
ENV API_KEY_POSTMARK=$API_KEY_POSTMARK

COPY . .

CMD 
