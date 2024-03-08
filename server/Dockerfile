FROM python:3.13.0a4-alpine3.19 AS builder

WORKDIR /app

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY ./server/requirements.txt .

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

COPY ./server/config ./ \
    ./server/tests ./ \
    ./server/__init__.py ./ \
    ./server/main.py ./

# final stage
FROM python:3.13.0a4-alpine3.19

ENV GROUP_ID=1001 \
    USER_ID=1001

RUN addgroup -g $GROUP_ID app && adduser -D -u $USER_ID -G app app -s /bin/sh

USER app

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/ /app/

WORKDIR /app

ENV PATH="/opt/venv/bin:$PATH"

EXPOSE 8080

CMD ["gunicorn", "-w", "1", "--bind", "0.0.0.0:8080", "main:application"]