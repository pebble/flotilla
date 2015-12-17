FROM alpine:3.2

RUN apk add --update \
    git \
    python \
    py-pip \
    py-dbus \
  && pip install virtualenv \
  && rm -rf /var/cache/apk/*


COPY requirements.txt /app/requirements.txt
RUN virtualenv --system-site-packages /env && /env/bin/pip install -r /app/requirements.txt

COPY src/ /app

WORKDIR /app
CMD ["/env/bin/python", "agent.py"]

