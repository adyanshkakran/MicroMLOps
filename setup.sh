#!/bin/bash
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

sudo docker-compose up -d
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Docker compose started${NC}"
else
    echo -e "${RED}Failed to start docker compose. Docker compose returned code $?${NC}"
    exit $?
fi

sudo docker cp ./topics.sh kafka1:/bin/topics.sh
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Copied topic creation script${NC}"
else
    echo -e "${RED}Failed to copy topic creation script. Docker returned code $?${NC}"
    exit $?
fi

sudo docker exec kafka1 /bin/topics.sh
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Topics created. Ready to use.${NC}"
else
    echo -e "${RED}Encountered an error when running topic creation script. Docker returned code $?${NC}"
    exit $?
fi

exit
