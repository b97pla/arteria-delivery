FROM python:3.7-alpine

RUN apk add git
WORKDIR /arteria-delivery

COPY . .
RUN pip install --no-cache-dir -r requirements/prod .


CMD ["delivery-ws", "--configroot=config/", "--port=8080", "--debug"]
