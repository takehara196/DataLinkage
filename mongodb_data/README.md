
# Note

```
$ cd mongodb_data/
$ docker ps
$ docker cp 2022-01-16.json {mongo: latest CONTAINER ID}/:data/
# docker exec -it {mongo: latest CONTAINER ID} /bin/bash
# cd /data
# ls
  2022-01-16.json  configdb  db
# mongoimport --port 27017 -u root -p password123 --authenticationDatabase=admin --db=sample --collection=zips --file=2022-01-16.json
```
