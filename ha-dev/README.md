# Development env for HA

To develop and test Channel Engine in HA mode using Redis as shared state store.

Start an NGINX as Load Balancer and a Redis instance:

```
cd ha-dev/
docker-compose -f docker-compose-lb.yml up --build 
```

The Load Balancer is then reachable on `http://localhost:8000` and will proxy (round robin) to three 
instances on port 8080, 8081 and 8082.

Start one engine instance with the following parameters (at least):

```
PORT=8080 REDIS_URL=redis://localhost:6379/ npm start
```

and a second engine instance:

```
PORT=8081 REDIS_URL=redis://localhost:6379/ npm start
```
