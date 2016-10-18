FROM quay.io/keboola/docker-base-php56:0.0.2
MAINTAINER Erik Zigo <erik.zigo@keboola.com>

# Install required tools
RUN yum install -y wget
RUN yum install -y tar
RUN yum install -y openssl
RUN yum -y --enablerepo=epel,remi,remi-php56 install php-devel
RUN yum -y --enablerepo=epel,remi,remi-php56 install php-pear
RUN yum -y --enablerepo=epel,remi,remi-php56 install php-mysql

WORKDIR /home

# Initialize
COPY . /home/
RUN composer install --no-interaction

RUN curl --location --silent --show-error --fail \
        https://github.com/Barzahlen/waitforservices/releases/download/v0.3/waitforservices \
        > /usr/local/bin/waitforservices && \
    chmod +x /usr/local/bin/waitforservices

ENTRYPOINT php ./run.php --data=/data