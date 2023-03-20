FROM ubuntu:20.04

RUN apt-get update && apt-get install -y gnupg2 curl postgresql-client redis-tools \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
    && /bin/bash -c "source ~/.bashrc" \
    && apt-get install -y python3-pip \
    && pip3 install --upgrade pip \
    && pip3 install torch matplotlib scikit-learn numpy pandas fastapi jupyter \
    && pip3 install docker apache-airflow \
    && curl -L "https://github.com/prometheus/prometheus/releases/download/v2.33.0/prometheus-2.33.0.linux-amd64.tar.gz" | tar -xz -C /tmp \
    && mv /tmp/prometheus-2.33.0.linux-amd64 /prometheus

RUN apt-get install -y openjdk-8-jdk

RUN curl -sSL https://get.docker.com/ | sh

RUN curl -sL https://deb.nodesource.com/setup_16.x | bash - \
    && apt-get install -y nodejs

RUN curl -sL https://deb.nodesource.com/setup_14.x | bash - \
    && apt-get install -y nodejs

RUN curl -sL https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
    && echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list \
    && apt-get update && apt-get install -y yarn

RUN apt-get install -y apache2-utils

EXPOSE 80 8888 9090 3000

CMD ["/bin/bash"]
