# hash:sha256:914c2ffa3f27747a2e8d5d2a45992c0d1ea6f6d83297e1b886b296f6e6ba8bfa
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/jupyterlab:3.6.1-miniconda4.12.0-python3.9-ubuntu20.04

ARG DEBIAN_FRONTEND=noninteractive
ARG GIT_ASKPASS
COPY git-askpass /

RUN pip install -U --no-cache-dir \
    aind-codeocean-utils==0.2.0 \
    codeocean==0.12.0 \
    python-dotenv==1.0.1
