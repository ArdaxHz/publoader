#!/bin/bash

echo "Monitoring for updates to publoader-extensions..."

while true; do
  sleep 30
  if docker ps --filter "label=com.centurylinklabs.watchtower.enable=true" | grep -q publoader-extensions; then
    echo "publoader-extensions updated. Restarting publoader..."
    docker compose restart publoader
  fi
done
