FROM python:3.8

RUN mkdir /app

COPY *.py /app/
COPY *.sh /app/
COPY variables.json /app/
COPY stations.json /app/
COPY requirements.txt /app/

RUN chown -R 1000:1000 /app
# RUN chmod -R 777 /app 

WORKDIR /app

RUN pip install -r /app/requirements.txt

USER 1000

EXPOSE 5000

CMD [ "sh", "-c", "./run.sh" ]
