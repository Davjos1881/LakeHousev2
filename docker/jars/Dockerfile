# Imagen minima: solo descarga los JARs en BUILD y los copia al volumen compartido al arrancar.
FROM alpine:3.20
RUN apk add --no-cache curl
COPY docker/common/download-jars.sh /tmp/download-jars.sh
RUN mkdir /jars && sh /tmp/download-jars.sh /jars && rm /tmp/download-jars.sh
