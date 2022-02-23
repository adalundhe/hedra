ARG REPO_SLUG
ARG REPO_BRANCH
ARG REPO_URL
ARG KUBE_CONFIG_PATH
ARG KUBE_CONFIG_FILE

#################################################################
FROM python:3.9.5-slim AS build-python
ENV PYTHONUNBUFFERED=0
ENV GRPC_DNS_RESOLVER=native

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git gcc openssh-server sudo \
    apt-transport-https gnupg2 curl\
    python3-dev build-essential nginx libpq-dev \
    && apt-get clean


COPY ./requirements.txt /requirements.txt

RUN pip install setuptools wheel ddtrace
RUN pip install -r /requirements.txt

################################################################
FROM python:3.9.5-slim-buster AS run
ARG REPO_SLUG
ARG REPO_URL
ARG REPO_BRANCH
ARG KUBE_CONFIG_PATH
ARG KUBE_CONFIG_FILE
ENV REPO_URL=${REPO_URL}
ENV REPO_SLUG=${REPO_SLUG}
ENV REPO_BRANCH=${REPO_BRANCH}

RUN mkdir /data && \
    mkdir /app && \
    mkdir /uwsgi && \
    mkdir /.kube

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git gcc curl sudo \
    apt-transport-https kubectx gnupg2 \
    python3-dev build-essential libpq-dev \
    && apt-get clean

RUN USERNAME="hedra" && \
    apt-get install -y sudo && \
    sudo groupadd -g 1000 $USERNAME && \
    useradd -u 1000 -g $USERNAME -s /bin/sh $USERNAME && \
    echo "$USERNAME ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/$USERNAME && \
    chmod 0440 /etc/sudoers.d/$USERNAME

COPY --from=build-python /usr/local/ /usr/local/
COPY config/nginx/* /etc/nginx/
COPY config/uwsgi/* /uwsgi/
COPY . /app/

RUN git clone --branch ${REPO_BRANCH} https://${REPO_URL}/${REPO_SLUG} "/data/${REPO_SLUG}"
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
RUN sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

WORKDIR /app

RUN python /app/setup.py develop

EXPOSE 6669 6670 6671 8711 9001 9002

RUN ["sudo", "chmod", "a+rw", "/data"]
RUN ["sudo","chmod", "a+x", "/app/scripts/hedra_entrypoint.sh"]

USER hedra

ENTRYPOINT ["/app/scripts/hedra_entrypoint.sh"]

