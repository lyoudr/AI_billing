FROM python:3.9-slim

ENV APP_SRC /workspace

WORKDIR $APP_SRC
COPY . /$APP_SRC

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

CMD ["flask", "--app", "ann_app", "run", "--host", "0.0.0.0", "--port", "80"]