FROM nvidia/cuda:10.2-cudnn7-devel-ubuntu18.04
ENV LANG C.UTF-8

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip python3 nano bash jq git gcc g++ && \
    pip3 install setuptools && \
    pip3 install wheel && \
    pip3 install torch && \
    pip3 install git+https://github.com/ThilinaRajapakse/simpletransformers

CMD ["/bin/bash"]
