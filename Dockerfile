FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY lobby.js ./
ENV NODE_ENV=production
EXPOSE 8080
CMD ["node", "lobby.js"]