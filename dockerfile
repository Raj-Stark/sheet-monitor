FROM node:22-alpine

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm ci --omit=dev

# Copy app code
COPY . .

# Ensure data directory exists
RUN mkdir -p /data

# Default command (Coolify Scheduled Task will also call this)
CMD ["node", "monitor.js"]
