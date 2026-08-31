FROM node:22.13.0-alpine

ARG RELEASE_LABEL
ENV RELEASE_LABEL=$RELEASE_LABEL

WORKDIR /opt/runku
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
COPY runner.mjs handler.mjs ./

USER 65532:65532
CMD ["node", "/opt/runku/runner.mjs"]
