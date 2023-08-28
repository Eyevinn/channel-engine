FROM node:18-alpine

WORKDIR /app

ADD . .

RUN npm install
RUN npm run build
ENV DEBUG=engine*

CMD ["npm", "start"]
