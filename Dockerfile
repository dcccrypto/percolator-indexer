# Builder stage
FROM node:22-alpine AS builder
ARG CACHE_BUST=20260403a
RUN apk add --no-cache python3 make g++ && rm -rf /var/cache/apk/*
RUN corepack enable && corepack prepare pnpm@10 --activate
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN echo "CACHE_BUST=$CACHE_BUST" && pnpm install --frozen-lockfile && pnpm ls @percolator/sdk
COPY tsconfig.json ./
COPY src ./src
RUN pnpm build

FROM node:22-alpine AS runner
RUN apk add --no-cache curl && rm -rf /var/cache/apk/*
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./
RUN chown -R node:node /app
USER node
ENV NODE_ENV=production
EXPOSE 3002
HEALTHCHECK --interval=30s --timeout=15s --start-period=10s --retries=3 CMD curl -f http://localhost:3002/health || exit 1
CMD ["node", "dist/index.js"]
