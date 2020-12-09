FROM openjdk:11.0.8-jdk

LABEL maintainer="JHC Systems"

WORKDIR /app

COPY ./ /app/

RUN chmod 755 entrypoint.sh
RUN chmod 770 /app

EXPOSE 8083

CMD ["sh", "entrypoint.sh"]
