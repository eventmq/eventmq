FROM python:2.7

ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install -y \
    ntp \
    build-essential \
    libssh-dev \
    libffi-dev \
    python-dev \
    libxmlsec1-dev \
    swig \
    swig2.0 \
    zsh \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

RUN mkdir -p /eventmq

ADD . /eventmq

WORKDIR /eventmq

RUN pip install -e .

ADD etc/eventmq.conf-dist /etc/eventmq.conf
