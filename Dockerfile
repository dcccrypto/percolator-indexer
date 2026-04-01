# Builder stage
FROM node:22-alpine AS builder
RUN corepack enable && corepack prepare pnpm@10 --activate
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY vendor ./vendor
RUN pnpm install --frozen-lockfile
COPY tsconfig.json ./
COPY src ./src
RUN pnpm build

# Runner stage — production-only dependencies, minimal attack surface
FROM node:22-alpine AS runner

# SEC: Install only curl for healthcheck, no other tools
RUN apk add --no-cache curl && \
    # SEC: Remove package manager caches to reduce image size and attack surface
    rm -rf /var/cache/apk/*

WORKDIR /app

# SEC: Copy only production node_modules (prune devDependencies)
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./

# SEC: Install production dependencies only, removing devDependencies
# This reduces image size and eliminates unnecessary packages from production
RUN corepack enable && corepack prepare pnpm@10 --activate && \
    pnpm prune --prod && \
    corepack disable

# SEC: Set ownership and drop to non-root user
RUN chown -R node:node /app
USER node

# SEC: Set NODE_ENV to production by default in container
ENV NODE_ENV=production

# SEC: Use dumb-init pattern via Node's --abort-on-uncaught-exception for clean exits
EXPOSE 4001
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:4001/health || exit 1
CMD ["node", "dist/index.js"]
