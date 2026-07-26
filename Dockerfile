# Builder stage
FROM node:22-alpine AS builder
ARG CACHE_BUST=20260403a
RUN apk add --no-cache python3 make g++ && rm -rf /var/cache/apk/*
RUN corepack enable && corepack prepare pnpm@10 --activate
WORKDIR /app
# #175: package.json/pnpm-lock.yaml resolve @percolatorct/sdk as
# `file:../percolator-sdk`. With WORKDIR=/app that is `/percolator-sdk`, which
# is not in a bare build context — install died with
# `ENOENT ... scandir '/percolator-sdk'` (exit 254). The CI docker job now
# checks the SDK into the context (see .github/workflows/ci.yml) and we place
# it at the path the lockfile expects. Must precede the install.
COPY percolator-sdk /percolator-sdk
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN echo "CACHE_BUST=$CACHE_BUST" && pnpm install --frozen-lockfile && pnpm ls @percolator/sdk
COPY tsconfig.json ./
COPY src ./src
RUN pnpm build

# #178: strip devDependencies (vitest, vite, tsx, @types/*) and their CVEs from
# the tree the runner inherits. The runner copies node_modules wholesale, so
# without this the production image ships the entire test toolchain.
#
# Pruning here rather than doing a --prod install in the runner (the pattern
# percolator-keeper uses): this repo resolves @percolatorct/sdk as
# `file:../percolator-sdk` and pulls in native optional deps, so a second
# install in the runner would need both the SDK in that stage's context and a
# build toolchain (python3/make/g++) that the runner deliberately lacks.
# Pruning reuses the tree that already built successfully.
RUN pnpm prune --prod

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
