FROM python:3-alpine3.15
WORKDIR /app
copy . /app
RUN pip install -r requirements.txt
EXPOSE 3000
CMD python ./app.py
