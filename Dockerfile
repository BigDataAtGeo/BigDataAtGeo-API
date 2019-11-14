FROM python:3.8-alpine

RUN mkdir /app

COPY *.py /app/
COPY *.sh /app/
COPY requirements.txt /app/

RUN chown -R 1000:1000 /app
# RUN chmod -R 777 /app 

WORKDIR /app

USER 1000

RUN pip install -r /app/requirements.txt

EXPOSE 5000

CMD [ "sh", "-c", "./run.sh" ]
