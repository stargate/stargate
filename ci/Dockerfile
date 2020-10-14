FROM ubuntu:bionic

RUN apt-get update && apt-get install openjdk-8-jdk git python2.7 python-setuptools python-six python-yaml sudo maven -y

RUN update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java && \
export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64" && \
export PATH=$PATH:$JAVA_HOME/bin && \
java -version

RUN ln -sf /usr/bin/python2.7 /usr/bin/python

RUN adduser --disabled-password --gecos "" ubuntu && \
usermod -aG sudo ubuntu && \
echo "ubuntu     ALL=(ALL) NOPASSWD:/usr/bin/python" >> /etc/sudoers

RUN su - ubuntu

RUN git clone --branch master --single-branch https://github.com/riptano/ccm.git && \
cd ccm && \
sudo python setup.py install

CMD ["/bin/bash"]