FROM node:12.16.1-alpine

WORKDIR /app

ADD . .

RUN npm install
ENV DEBUG=engine*

CMD ["npm", "start"]