# Use a Python 3.11 base image
FROM python:3.11

# Set the working directory
WORKDIR /usr/app/src

# Copy and install dependencies
COPY ./requirements.txt /usr/app/src/requirements.txt

COPY exn /usr/app/exn/


RUN apt-get update && \
    apt-get install -y netcat-traditional vim iputils-ping curl telnet && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install -r /usr/app/src/requirements.txt

# Solve the problem in _trio.py after install Python dependencies (exceptiongroup)
RUN sed -i 's/class ExceptionGroup(BaseExceptionGroup, trio.MultiError):/class ExceptionGroup(BaseExceptionGroup):/' /usr/local/lib/python3.11/site-packages/anyio/_backends/_trio.py
	
# Copy the rest of the application
COPY ./ .