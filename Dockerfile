FROM postgres:10-alpine

ENV POSTGRES_PASSWORD password
ENV POSTGRES_USER postgres
ENV POSTGRES_DB postgres

ADD ./docker/user-databases.txt /usr/local/bin/
ADD ./docker/setup-databases.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/setup-databases.sh
RUN /usr/local/bin/setup-databases.sh


EXPOSE 5432