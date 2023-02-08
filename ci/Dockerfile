FROM ubuntu:focal

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update \
    && apt-get install -y \
    openjdk-8-jdk \
    openjdk-17-jdk \
    git \
    gcc \
    python-dev \
    python2.7 \
    python-setuptools \
    python-six \
    python-yaml \
    sudo \
    maven \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable" \
    && apt-get update \
    && apt-cache policy docker-ce \
    && apt-get install docker-ce -y

RUN update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java \
    && export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64" \
    && export PATH=$PATH:$JAVA_HOME/bin \
    && java -version

# psutil needed as an explicit dep of the ccm
# see https://github.com/riptano/ccm#requirements
RUN ln -sf /usr/bin/python2.7 /usr/bin/python \
    && curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py \
    && python2.7 get-pip.py \
    && pip install psutil

RUN adduser --disabled-password --gecos "" ubuntu \
    && usermod -aG sudo ubuntu \
    && echo "ubuntu     ALL=(ALL) NOPASSWD:/usr/bin/python" >> /etc/sudoers \
    && usermod -aG docker ubuntu \
    && su - ubuntu

RUN git clone --branch master --single-branch https://github.com/riptano/ccm.git && \
    cd ccm && \
    sudo python setup.py install

# Create clusters to pre-download necessary artifacts
RUN ccm create -v 4.0.7 stargate_40 && ccm remove stargate_40
RUN ccm create -v 3.11.14 stargate_311 && ccm remove stargate_311
RUN ccm create -v 6.8.32 --dse stargate_dse68 && ccm remove stargate_dse68

# init the maven user home dir with correct permissions
# needed for running maven wrapper
RUN mkdir /home/ubuntu/.m2/ \
    && chown ubuntu:ubuntu /home/ubuntu/.m2/

CMD ["/bin/bash"]
