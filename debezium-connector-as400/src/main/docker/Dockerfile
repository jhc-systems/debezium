FROM docker-repo.neon-connect.com:5000/jhc_java

LABEL maintainer="JHC Systems"

WORKDIR /app

COPY ./ /app/

RUN chmod 755 entrypoint.sh

EXPOSE 8083

CMD ["sh", "entrypoint.sh"]
