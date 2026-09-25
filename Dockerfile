# Use official Node.js 22 LTS (Alpine Linux for lightweight footprint)
FROM node:22-alpine

# Set working directory
WORKDIR /app

# Copy dependency manifests
COPY package*.json ./

# Install production dependencies
RUN npm ci --only=production

# Copy application source code
COPY . .

# Expose server port (default 3000, configurable via PORT env var)
EXPOSE 3000

# Set environment variables
ENV NODE_ENV=production
ENV PORT=3000

# Start VaultMesh server with SQLite & WebSockets
CMD ["npm", "start"]
