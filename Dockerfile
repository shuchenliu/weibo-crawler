FROM python:3.6

MAINTAINER Will Liu<scliu@scripps.edu>

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

CMD python routine.py >> /usr/logs/super.log