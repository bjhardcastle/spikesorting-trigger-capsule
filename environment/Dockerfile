# hash:sha256:5808996b36165c29c8c2404ffd9f3a7557d60046a5834c1ad668a1c1fb65dcb8
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/jupyterlab:3.6.1-miniconda4.12.0-python3.9-ubuntu20.04

ARG DEBIAN_FRONTEND=noninteractive
ARG API_KEY
ARG API_SECRET
ARG AWS_ACCESS_KEY_ID
ARG AWS_DEFAULT_REGION
ARG AWS_SECRET_ACCESS_KEY

ARG GIT_ASKPASS
ARG GIT_ACCESS_TOKEN
COPY git-askpass /

RUN pip install -U --no-cache-dir \
    aind-codeocean-api==0.4.0 \
    aind-codeocean-utils==0.2.0 \
    python-dotenv==1.0.1
