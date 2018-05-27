FROM jackytck/docker-node-imagemagick:v0.0.1

RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        python \
    && pip3 install --upgrade pip \
    && apt-get clean
RUN pip3 --no-cache-dir install --upgrade awscli

WORKDIR /app
COPY package.json .
RUN yarn install --production

COPY dist dist

CMD ["yarn", "start"]
