FROM bitnami/minideb:latest

RUN apt-get update
RUN apt-get install -y wget curl bzip2
ENV PATH="/root/miniconda3/bin:${PATH}"
RUN wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash ./Miniconda3-latest-Linux-x86_64.sh -b
RUN /root/miniconda3/bin/conda install -y python=3.6
RUN rm ./Miniconda3-latest-Linux-x86_64.sh

ADD VERSION .

RUN python -m ensurepip && \
    rm -rf /usr/lib/python*/ensurepip && \
    pip install --upgrade pip setuptools && \
    rm -r /root/.cache

RUN pip install numpy pandas
RUN pip install websocket-client python-rapidjson kafka-python
RUN pip install https://github.com/deribit/deribit-api-python/archive/master.zip
COPY do.py /root/
COPY lib.py /root/
WORKDIR /root
# RUN apt-get install -y git
# RUN pip install git+https://github.com/deribit/deribit-api-python.git
# RUN pip install pandas --no-build-isolation

# TODO docker secrets for .cred
