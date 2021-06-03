# Introduction to Confluent Kafka

## Setup Confluent Kafka Locally
Refer to the link below (Image used CentOS 7.8.2003 - centos-release-7-8.2003.0.el7.centos.x86_64)
https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html

```bash
# Ensure connected to internet by ping

sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install -y docker-ce docker-ce-cli containerd.io

sudo systemctl start docker

# Install docker-compose (Optionally, create Symlink for docker-compose)
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Pull image and run confluent Kafka
curl --silent --output docker-compose.yml https://raw.githubusercontent.com/confluentinc/cp-all-in-one/6.1.1-post/cp-all-in-one/docker-compose.yml
sudo /usr/local/bin/docker-compose pull
sudo /usr/local/bin/docker-compose up -d

# To use ksqlDB in Control Center, setup SSH tunnelling using source 8088 to 127.0.0.1:8088
# Otherwise connect to docker container (not tried)
docker-compose exec ksqldb-cli ksql http://ksqldb-server:8088
```

## Setup Local Producer and Consumer

**Setup SSH tunnelling using PuTTY using source 9092 to 127.0.0.1:9092**

1. Go into WebServer directory `cd WebServer`
2. Install dependencies using `npm install`
3. Start the producer/consumer using `npm start`
4. Go to http://localhost:3000

## Setup Entire Docker Compose Stack

_Optional - Create Symbolic Link to docker-compose_
1. To Start `docker-compose up --build -d`
2. To Check `docker-compose ps`
3. To Stop `docker-compose down`
