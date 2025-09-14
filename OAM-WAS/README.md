- docker network create oam-net
- docker build -t rum-backend:dev .
```
docker run --rm -p 9500:9500 \
-e DD_API_KEY=<API_KEY> \
-e DD_APP_KEY=<APP_KEY> \
-e DD_SITE=ap1.datadoghq.com \
--name oam-was-container --network oam-net rum-backend:dev
```