FROM python:3.11-slim-bookworm

# This variable is used to dynamically set the version of the uv tool being downloaded and installed later.
# UV tool is a faster version of pip.
ARG UV_VERSION=0.5.0

# Updates the list of available packages from the Debian repository and 
# install curl - A command-line tool for downloading files from URLs
RUN apt-get -y update; apt-get -y install curl 

# Adds a file from a remote URL (https://astral.sh/uv/${UV_VERSION}/install.sh) to the container's file system at /install.sh.
# --chmod=755 sets the file permissions so the script is executable.
ADD --chmod=755 https://astral.sh/uv/${UV_VERSION}/install.sh /install.sh
RUN /install.sh && rm /install.sh

COPY requirements.txt /requirements.txt

RUN /root/.local/bin/uv pip install --system --no-cache -r requirements.txt

WORKDIR /sqliac

COPY . /sqliac

CMD ["python", "/sqliac/main.py"]
