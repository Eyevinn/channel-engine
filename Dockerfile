FROM node:slim

WORKDIR /app

ADD . .

RUN npm install
ENV DEBUG=engine-sessionLive

CMD ["npm", "start"]