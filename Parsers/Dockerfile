FROM python:3.6
MAINTAINER Amalie Trewartha "AmalieTrewartha@lbl.gov"
WORKDIR /user/src/app
COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN mkdir /user/src/app/Parsers
RUN groupadd nginx
RUN chgrp nginx /user/src/app/Parsers
RUN chmod -R g+wrx /user/src/app/Parsers
WORKDIR /user/src/app/Parsers

COPY * /user/src/app/Parsers/
COPY pdf_extractor/ /user/src/app/Parsers/pdf_extractor/
COPY utils/ /user/src/app/Parsers/utils/
ARG COVID_HOST
ARG COVID_PASS
ARG COVID_USER
ARG COVID_DB
ENV COVID_HOST $COVID_HOST
ENV COVID_USER $COVID_USER
ENV COVID_PASS $COVID_PASS
ENV COVID_DB $COVID_DB

EXPOSE 8080

CMD [ "python", "run_all_parsers.py"]
