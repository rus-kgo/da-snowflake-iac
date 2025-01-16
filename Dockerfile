FROM python:3.11-slim-bookworm

ARG UV_VERSION=0.5.0

RUN apt-get -y update; apt-get -y install curl 

ADD --chmod=755 https://astral.sh/uv/${UV_VERSION}/install.sh /install.sh
RUN /install.sh && rm /install.sh

COPY requirements.txt /requirements.txt

RUN /root/.local/bin/uv pip install --system --no-cache -r requirements.txt

WORKDIR /github/workspace/snowflake-iac

COPY . /github/workspace/snowflake-iac

# CMD ["python", "/main.py"]
CMD ["/bin/sh", "-c", "ls -R  /github/workspace/snowflake-iac && python /github/workspace/snowflake-iac/main.py"]
