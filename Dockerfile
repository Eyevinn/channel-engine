FROM node:24-alpine

WORKDIR /app

ADD . .

RUN npm install
RUN npm run build
ENV DEBUG=engine*

CMD ["npm", "start"]
