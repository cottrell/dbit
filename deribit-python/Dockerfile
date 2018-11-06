FROM bitnami/minideb:latest

RUN apt-get update
RUN apt-get install -y wget curl bzip2
ENV PATH="/root/miniconda3/bin:${PATH}"
RUN wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash ./Miniconda3-latest-Linux-x86_64.sh -b
RUN /root/miniconda3/bin/conda install -y python=3.6

# RUN python3 -m ensurepip && \
#     rm -rf /usr/lib/python*/ensurepip && \
#     pip3 install --upgrade pip setuptools && \
#     if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi && \
#     if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python3 /usr/bin/python; fi && \
#     rm -r /root/.cache
# RUN pip install pandas --no-build-isolation
