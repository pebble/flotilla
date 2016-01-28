#!/bin/bash

AWS_CREDS="-v ${HOME}/.aws/credentials:/root/.aws/credentials "
# -e AWS_ACCESS_KEY_ID= -e AWS_SECRET_ACCESS_KEY= would also work

FLOTILLA="pwagner/flotilla"
REGIONS="-r us-east-1"
# -r us-east-1 -r us-west-2 would also work

# Update:
docker pull ${FLOTILLA}

# Initialize environment:
docker run ${AWS_CREDS} ${FLOTILLA} init ${REGIONS} --domain mycloudand.me

# Create user(s):
docker run ${AWS_CREDS} ${FLOTILLA} user ${REGIONS} --name pwagner --ssh-key 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC0b/hxxZesuGbCH7DBn299BZSwcviBFyPPpSm1+5ygO0j1qKoekt4Ou4PfHqBQtXuxyEKTPW1TXhUV764nwgUlPA0qs4tHB7NcKBiFCMr6I2RBohhiYk1Ed3XvvOR4W9Q3KrueBScXMLYBU0aKNpViR5i7WStkPsIemgE8uh73sDNPKRfzAuKz53qbqaqtEwPP8l25e85LfrCNOf4mBGTb1EO3GQccgXlbnOa3UDM1iQLRk/1bcSQN7ezrppGuvDkg4p73w+go34ZWCRUzWUcro0ZYUjty+GMzq6Chv8rdqc2MoCzuUZ356Nq3F0sbFVclGPNkEt46whyMDG43YY6j'

# Setup admins:
docker run ${AWS_CREDS} ${FLOTILLA} region ${REGIONS} --admin pwagner

# Add a service:
docker run ${AWS_CREDS} ${FLOTILLA} service ${REGIONS} --name elasticsearch --public-ports 9200-http --private-ports 9300-tcp --health-check HTTP:9200/ --instance-min 2 --instance-max 3

# Add a revision of that service:
tar -cf - elasticsearch.env | docker run -i ${AWS_CREDS} ${FLOTILLA} revision ${REGIONS} --name elasticsearch --label initial

