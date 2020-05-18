FROM python:3-slim

RUN apt-get update && \
    apt-get install -y nano jq bash git

WORKDIR /app
COPY . .

CMD ["/bin/bash"]
