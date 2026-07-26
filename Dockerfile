# Builder stage
FROM node:22-alpine AS builder
ARG CACHE_BUST=20260403a
RUN apk add --no-cache python3 make g++ && rm -rf /var/cache/apk/*
RUN corepack enable && corepack prepare pnpm@10 --activate
WORKDIR /app
# #175 is resolved at the source: @percolatorct/sdk is a published npm package
# now, not a `file:../percolator-sdk` link, so nothing has to be staged into the
# build context first — the install resolves it from the registry.
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
# The `pnpm ls` guard named `@percolator/sdk`, which is not a dependency of this
# package (the scope is `@percolatorct`), so it matched nothing and exited 0 —
# it never verified anything. Assert on the real name, and fail if it is absent.
RUN echo "CACHE_BUST=$CACHE_BUST" && pnpm install --frozen-lockfile \
    && pnpm ls @percolatorct/sdk | grep -q '@percolatorct/sdk'
COPY tsconfig.json ./
COPY src ./src
RUN pnpm build

# #178: strip devDependencies (vitest, vite, tsx, @types/*) and their CVEs from
# the tree the runner inherits. The runner copies node_modules wholesale, so
# without this the production image ships the entire test toolchain.
#
# Pruning here rather than doing a --prod install in the runner (the pattern
# percolator-keeper uses): the dependency tree pulls in native optional deps, so
# a second install in the runner would need a build toolchain (python3/make/g++)
# that the runner deliberately lacks. Pruning reuses the tree that already built
# successfully.
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
