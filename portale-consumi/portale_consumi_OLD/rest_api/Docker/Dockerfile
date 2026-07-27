FROM python:3.6.5
ADD api.py api.py
RUN apt-get update && apt-get install -y \
    sasl2-bin libsasl2-2 libsasl2-dev \
    libsasl2-modules && \
    rm -rf /var/lib/apt/lists/*
ADD requirements.txt requirements.txt 
RUN pip install --upgrade pip &&\
    pip install -r requirements.txt
ENTRYPOINT ["python","/api.py"]
