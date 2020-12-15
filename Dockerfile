FROM python:3.6.10
MAINTAINER Amalie Trewartha "amalietrewartha@lbl.gov"
WORKDIR /user/src/app
COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.3.0/en_core_sci_lg-0.3.0.tar.gz
RUN git clone https://github.com/COVID-19-Text-Mining/DBProcessingScripts
COPY ML_builders/COVID19_Binary_430_2 /user/src/app/DBProcessingScripts/ML_builders/COVID19_Binary_430_2

CMD [ "bash","DBProcessingScripts/entries_script.bash" ]
