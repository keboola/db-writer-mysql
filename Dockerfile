FROM keboola/db-component-ssh-proxy:latest AS sshproxy
FROM php:7.1-cli
ARG DEBIAN_FRONTEND=noninteractive
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ENV COMPOSER_ALLOW_SUPERUSER=1
ENV COMPOSER_PROCESS_TIMEOUT 3600

# Deps
RUN apt-get update
RUN apt-get install -y wget curl make git bzip2 time libzip-dev openssl unzip

# PHP
RUN docker-php-ext-install pdo pdo_mysql
RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini
RUN echo "date.timezone = \"Europe/Prague\"" >> /usr/local/etc/php/php.ini

# Composer
WORKDIR /root
RUN curl -sS https://getcomposer.org/installer | php \
  && mv composer.phar /usr/local/bin/composer

# Main
WORKDIR /code
## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

COPY --from=sshproxy /root/.ssh /root/.ssh
CMD php ./run.php --data=/data
