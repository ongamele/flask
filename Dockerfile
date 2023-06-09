FROM python:3.6
#FROM registry.access.redhat.com/ubi8/python-36
#FROM registry.access.redhat.com/ubi8

WORKDIR /app

#COPY Pipfile* /app/

## NOTE - rhel enforces user container permissions stronger ##
#USER root
RUN export DEBIAN_FRONTEND="noninteractive"
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -y install --no-install-recommends wget swig && rm -rf /var/lib/apt/lists/*

#RUN pip install --upgrade pip \
#  && pip install --upgrade pipenv\
#  && pip install -q cython\
#  && pip install flask-cors\
#  && pipenv install --system --deploy

#USER 1001
COPY requirements.txt /app

RUN pip install -r requirements.txt

#COPY . /app
ENV FLASK_APP=server/__init__.py
EXPOSE 3000
CMD ["python3", "manage.py", "start", "0.0.0.0:3000"]
