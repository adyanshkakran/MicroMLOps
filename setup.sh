#!/bin/bash
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Make folders for kafka volumes
mkdir -p kafka/data kafka/logs
sudo chown -R 1000:1000 kafka/data
sudo chown -R 1000:1000 kafka/logs

# Spin up docker compose
sudo docker-compose up --build -d
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Docker compose started${NC}"
else
    echo -e "${RED}Failed to start docker compose. Docker compose returned code $?${NC}"
    exit $?
fi

# Copy topics creating script to kafka container
sudo docker cp ./topics.sh kafka1:/bin/topics.sh
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Copied topic creation script${NC}"
else
    echo -e "${RED}Failed to copy topic creation script. Docker returned code $?${NC}"
    exit $?
fi

# Create topics using script
sudo docker exec kafka1 /bin/topics.sh
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Topics created. Ready to use.${NC}"
else
    echo -e "${RED}Encountered an error when running topic creation script. Docker returned code $?${NC}"
    exit $?
fi

if [ "$1" = "--logs" -o "$1" = "-l" ]; then
    sudo docker-compose logs -f
fi
exit
