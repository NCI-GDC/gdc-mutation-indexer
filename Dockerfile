FROM ubuntu:18.04

# environment
ENV SPARK_VERSION 2.4.5
ENV ES_VERSION 7.6.2
ENV BOTO_CONFIG /dev/null
ENV JAVA_HME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH "${JAVA_HOME}/bin:${PATH}"
ENV SPARK_HOME /root/spark-${SPARK_VERSION}-bin-hadoop2.7
ENV PYTHONPATH $SPARK_HOME/python/:$PYTHONPATH
ENV PYTHONPATH $SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH

COPY . /app
RUN mkdir /root/.ssh
RUN mv /app/id_rsa /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa

# core setup
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk python-pip unzip git ssh
RUN ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts
WORKDIR /app
RUN pip install -r requirements.txt
RUN pip install -r dev-requirements.txt
RUN python setup.py develop

# es setup
WORKDIR /root
RUN groupadd -g 1000 elasticsearch && useradd elasticsearch -u 1000 -g 1000
RUN wget --progress=bar:force https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${ES_VERSION}-amd64.deb
RUN dpkg --force-confnew -i elasticsearch-${ES_VERSION}-amd64.deb
RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install mapper-size
RUN sed -i 's/^-Xms.*/-Xms2g/' /etc/elasticsearch/jvm.options
RUN sed -i 's/^-Xmx.*/-Xmx2g/' /etc/elasticsearch/jvm.options
RUN chown elasticsearch /etc/default/elasticsearch
RUN service elasticsearch restart

# spark setup
WORKDIR /root
RUN wget --progress=bar:force https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
RUN tar -xzvf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
RUN wget --progress=bar:force https://artifacts.elastic.co/downloads/elasticsearch-hadoop/elasticsearch-hadoop-${ES_VERSION}.zip
RUN unzip elasticsearch-hadoop-${ES_VERSION}.zip

RUN cp /root/elasticsearch-hadoop-${ES_VERSION}/dist/elasticsearch-spark-20_2.11-${ES_VERSION}.jar $SPARK_HOME/jars
