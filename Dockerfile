FROM golang:1.15.5 AS build
WORKDIR /build
COPY . .
RUN go mod download
RUN go build main.go

FROM python:3.7-slim
RUN pip3 install --no-cache-dir tqdm pandas
WORKDIR /src
COPY run.sh .
COPY preprocessing.py .
COPY --from=build /build/main ./main
RUN chown -R 1000:1000 .
RUN chmod +x run.sh

USER 1000

EXPOSE 5000

ENTRYPOINT sh -c ./run.sh