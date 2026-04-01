# Builder stage — rebuilt 20260401-0808
FROM node:22-alpine AS builder
RUN apk add --no-cache python3 make g++ && rm -rf /var/cache/apk/*
RUN corepack enable && corepack prepare pnpm@10 --activate
WORKDIR /app
ADD vendor.tar.gz .
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN ls -la vendor/percolator-sdk/ && pnpm install --frozen-lockfile
COPY tsconfig.json ./
COPY src ./src
RUN pnpm build

FROM node:22-alpine AS runner
RUN apk add --no-cache curl && rm -rf /var/cache/apk/*
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./
RUN corepack enable && corepack prepare pnpm@10 --activate && pnpm prune --prod && corepack disable
RUN chown -R node:node /app
USER node
ENV NODE_ENV=production
EXPOSE 4001
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD curl -f http://localhost:4001/health || exit 1
CMD ["node", "dist/index.js"]
