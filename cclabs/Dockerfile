FROM python:3.6-alpine3.7

ENV PATH=$PATH:/root/.local/bin
ENV AWS_DEFAULT_PROFILE=prod
ENV AWS_DEFAULT_REGION=us-east-1

RUN apk add --no-cache \
            git && \
    mkdir -p /opt/aws-cost-analysis && cd /opt/aws-cost-analysis && \
    git clone https://github.com/concurrencylabs/aws-cost-analysis.git . && \
    pip install -r requirements.txt && \
    pip install awscli --upgrade --user && \

VOLUME /root/.aws

WORKDIR /opt/aws-cost-analysis/scripts
