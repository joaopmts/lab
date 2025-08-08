## PREREQUISITES
#### Install Git and Docker
   * Install Docker Desktop on Windows [Docker Desktop](https://hub.docker.com/editions/community/docker-ce-desktop-windows) or Docker on [Linux](https://docs.docker.com/install/linux/docker-ce/ubuntu/)
   * [Git Installation](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

### To check running containers
         docker ps 

### To stop a container
         docker stop [container name]      

### To stop all containers
         docker stop $(docker ps -a -q)
  
### To remove a container
         docker rm [container name]

### To remove all containers
         docker rm $(docker ps -a -q)         

### Container details
         docker container inspect [container name]

### Start a specific container
         docker-compose up -d [container name]

### Start all containers (CAUTION: very heavy)
         docker-compose up -d 

### Access container logs
         docker container logs [container name] 

## WebUI Access for Frameworks
 
* Minio *http://localhost:9051*
* Jupyter Spark *http://localhost:8889*
* Pinot *http://localhost:9000*
* Nifi *http://localhost:9090*
* Kafka Control Center *http://localhost:9021*
* Elastic *http://localhost:9200*
* Metabase *http://localhost:3000*
* Kibana *http://localhost:5601*
* Superset *http://localhost:8088*
* Trino *http://localhost:8080*
* Hadoop *http://localhost:9870*
* Hive *http://localhost:10002*
* Airflow *http://localhost:8089*


## Users and Passwords
   ##### Airflow
    User: admin
    Password: admin

   ##### Superset
    User: admin
    Password: admin

   ##### Metabase
    User: admin@mds.com
    Password: admin 

   ##### Postgres
    User: admin
    Password: admin
   
   ##### Minio
    User: admin
    Password: minioadmin
       
   ##### Pinot
    User: admin
    Password: admin
        
   ##### Kibana
    User: admin
    Password: admin
        
   ##### Hue
    User: admin
    Password: admin
