# --- Stage 1: Build the React Application ---
# Use a Node.js image as the builder.
# The 'alpine' version is lightweight.
FROM node:18-alpine AS builder

# Set the working directory inside the container.
WORKDIR /app

# Copy the package.json and package-lock.json (if available) first.
# This takes advantage of Docker's layer caching.
COPY package.json ./
# If you have a package-lock.json, copy it as well.
# COPY package-lock.json ./

# Install dependencies.
# The user will need to run this step themselves, but it's here for the Docker build.
# NOTE: This command will be run by the user when they build the image.
RUN npm install

# Copy the rest of the application code.
COPY . .

# Build the application for production.
# This will create a 'dist' directory with the optimized static files.
RUN npm run build


# --- Stage 2: Serve the Application with Nginx ---
# Use a lightweight Nginx image.
FROM nginx:1.21-alpine

# Copy the built static files from the 'builder' stage.
COPY --from=builder /app/dist /usr/share/nginx/html

# Copy a custom Nginx configuration file.
# This will handle routing and proxying.
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Expose port 80 for the Nginx server.
EXPOSE 80

# The default Nginx command will start the server.
CMD ["nginx", "-g", "daemon off;"]
