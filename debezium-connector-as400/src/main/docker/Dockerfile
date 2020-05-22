FROM docker-repo.neon-connect.com:5000/jhc_java

LABEL maintainer="JHC Systems"

RUN useradd -d /app -m restapi

WORKDIR /app

COPY --chown=restapi:restapi maven/ /app/

USER restapi

EXPOSE 8083

CMD ["sh", "entrypoint.sh"]
