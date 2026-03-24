FROM node:25-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev

COPY src/ src/

USER node

ENV NODE_ENV=production

EXPOSE 8080

ENTRYPOINT ["tini", "--"]
CMD ["node", "--experimental-strip-types", "--expose-gc", "--max-old-space-size=2048", "src/main.ts"]
