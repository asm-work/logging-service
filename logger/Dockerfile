FROM python:3.13.0a4-alpine3.19 AS builder

WORKDIR /logger

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY ./logger/requirements.txt .

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

COPY ./logger/config ./ \
    ./logger/message_broker ./ \
    ./logger/tests ./ \
    ./logger/main.py ./ \
    ./logger/pytest.ini ./ \
    ./logger/scripts ./

# final stage
FROM python:3.13.0a4-alpine3.19

ENV GROUP_ID=1001 \
    USER_ID=1001

RUN addgroup -g $GROUP_ID logger && adduser -D -u $USER_ID -G logger logger -s /bin/sh

USER logger

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /logger /logger

WORKDIR /logger

ENV PATH="/opt/venv/bin:$PATH"

ENTRYPOINT ["python", "main.py"]