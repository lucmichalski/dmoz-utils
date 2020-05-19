FROM python:3-slim

RUN apt-get update && \
    apt-get install -y nano bash jq

WORKDIR /app
COPY . . 

RUN pip install -r requirements.txt

CMD ["/bin/bash"]
