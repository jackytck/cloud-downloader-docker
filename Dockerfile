FROM node:9-slim

RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
    && pip3 install --upgrade pip \
    && apt-get clean
RUN pip3 --no-cache-dir install --upgrade awscli

WORKDIR /app
COPY package.json .
RUN yarn install --production

COPY src src

CMD ["yarn", "start"]
