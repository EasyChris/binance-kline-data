# syntax=docker/dockerfile:1

FROM python:3.7

WORKDIR /usr/src/app

RUN /usr/local/bin/python -m pip install --upgrade pip

RUN pip install ccxt pandas requests zipfile36 pymongo

CMD [ "python3", "/usr/src/app/code/data_index.py"]