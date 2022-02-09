
# Note

```
$ cd mongodb_data/
$ docker ps
$ docker cp 2022-01-16.json {mongo: latest CONTAINER ID}/:data/
$ docker exec -it {mongo: latest CONTAINER ID} /bin/bash
{mongo: latest CONTAINER ID}:/# cd /data/
# ls
  2022-01-16.json  configdb  db
# mongoimport --port 27017 -u root -p password123 --authenticationDatabase=admin --db=sample --collection=zips --file=2022-01-16.json
2022-02-07T16:31:15.559+0000    connected to: mongodb://localhost:27017/
2022-02-07T16:31:18.596+0000    [#####...................] sample.zips  6.58MB/30.6MB (21.5%)
2022-02-07T16:31:21.560+0000    [#######.................] sample.zips  9.83MB/30.6MB (32.1%)
2022-02-07T16:31:24.561+0000    [###########.............] sample.zips  14.2MB/30.6MB (46.3%)
2022-02-07T16:31:27.561+0000    [#############...........] sample.zips  17.7MB/30.6MB (57.6%)
^[[D2022-02-07T16:31:28.812+0000        [########################] sample.zips  30.6MB/30.6MB (100.0%)
2022-02-07T16:31:28.812+0000    33741 document(s) imported successfully. 0 document(s) failed to import.
```
