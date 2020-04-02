FROM python:3.6
MAINTAINER Amalie Trewartha "AmalieTrewartha@lbl.gov"
WORKDIR /user/src/app
COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN mkdir /user/src/app/Parsers
RUN groupadd nginx
RUN chgrp nginx /user/src/app/Parsers
RUN chmod -R g+wrx /user/src/app/Parsers
WORKDIR /user/src/app/PARSERS

COPY * /user/src/app/Parsers
ARG COVIDSCHOLAR_HOST
ARG COVIDSCHOLAR_PASS
ARG COVIDSCHOLAR_USER
ARG COVIDSCHOLAR_DB
ENV COVIDSCHOLAR_HOST $COVIDSCHOLAR_HOST
ENV COVIDSCHOLAR_USER $COVIDSCHOLAR_USER
ENV COVIDSCHOLAR_PASS $COVIDSCHOLAR_PASS
ENV COVIDSCHOLAR_DB $COVIDSCHOLAR_DB

EXPOSE 8080

CMD [ "python", "run_all_parsers.py"]
