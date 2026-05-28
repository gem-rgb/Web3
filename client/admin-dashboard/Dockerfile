FROM node:20-alpine AS build
WORKDIR /app

COPY client/admin-dashboard/package.json client/admin-dashboard/tsconfig.json client/admin-dashboard/tsconfig.node.json client/admin-dashboard/vite.config.ts ./ 
COPY client/admin-dashboard/index.html ./index.html
COPY client/admin-dashboard/src ./src

RUN npm install
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

