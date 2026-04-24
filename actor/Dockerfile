FROM apify/actor-node-playwright-chrome:20

COPY package*.json ./
RUN npm --quiet set progress=false \
    && npm install --only=prod --no-optional \
    && echo "npm install finished"

COPY . ./

CMD npm start --silent
