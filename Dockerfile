# base image
FROM python:3.11.10-alpine

# set working directory
ENV LAMBDA_TASK_ROOT=/usr/src/app
WORKDIR /usr/src/app

# Run all setup for system, application, and files
RUN apk add --no-cache --virtual build-deps build-base libtool autoconf automake make cmake gcc musl-dev linux-headers libstdc++ libpq curl elfutils-dev && \
    pip install --no-cache-dir -U pip awslambdaric serverless-wsgi && \
    # apk del --purge build-deps build-base libtool autoconf automake make cmake gcc musl-dev linux-headers libstdc++ libpq curl elfutils-dev && \
    rm -rf /var/cache/apk/*

# Copy all application files
COPY . /usr/src/app

# Install app requirements
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

# Pass the name of the function handler as an argument to the runtime
CMD [ "manage.handler" ]
