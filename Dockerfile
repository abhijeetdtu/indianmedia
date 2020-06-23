#Grab the latest alpine image
#FROM fastgenomics/sklearn:0.19.1-p36-v5
FROM python:3.7.6-slim-buster

# Install python and pip
ADD ./requirements.txt /tmp/requirements.txt

# Install dependencies
RUN pip3 install --upgrade pip setuptools wheel && \
 pip3 install -r /tmp/requirements.txt

#RUN apk del build_deps

# Add our code
ADD . /opt/webapp/
WORKDIR /opt/webapp

# Expose is NOT supported by Heroku
# EXPOSE 5000

# Run the app.  CMD is required to run on Heroku
# $PORT is set by Heroku
CMD gunicorn --bind 0.0.0.0:$PORT IndianMedia.Web.main
