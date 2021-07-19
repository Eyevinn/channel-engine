FROM node:slim

WORKDIR /app

ADD . .

RUN npm install

CMD ["npm", "start"]