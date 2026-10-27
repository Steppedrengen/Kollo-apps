ARG BUILD_FROM
FROM $BUILD_FROM
RUN apk add --no-cache python3
COPY server.py /server.py
COPY ui.html /ui.html
CMD ["python3", "-u", "/server.py"]
