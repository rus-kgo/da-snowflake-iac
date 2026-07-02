FROM python:3.11-slim-bookworm

# This variable is used to dynamically set the version of the uv tool being downloaded and installed later.
# UV tool is a faster version of pip.
ARG UV_VERSION=0.5.0

# Update package lists, install curl, and clean up apt cache to reduce image size.
RUN apt-get update -y \
    && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*

# Add and run the uv installation script.
ADD --chmod=755 https://astral.sh/uv/${UV_VERSION}/install.sh /install.sh
RUN /install.sh && rm /install.sh

# Set the working directory for the application.
# This ensures subsequent commands like `uv pip install .` correctly find your pyproject.toml.
WORKDIR /sqliac

# Copy only essential files needed for dependency installation.
# This leverages Docker's build cache: if only pyproject.toml or src/ changes,
# only this layer and subsequent layers are rebuilt, not the uv installation.
COPY pyproject.toml ./
COPY src/ ./src/

# Install project dependencies from pyproject.toml using uv.
# The '.' indicates to install the package defined in the current directory's pyproject.toml.
# `--system` installs into the base Python environment.
# `--no-cache` prevents uv from caching wheels, saving image space.
RUN /root/.local/bin/uv pip install --system --no-cache .

# Copy the rest of the application code.
# This is done after dependency installation to maximize cache hits if only code changes.
COPY . .

# Define the default command to run when the container starts.
CMD ["python", "-m", "sqliac", "apply"]
