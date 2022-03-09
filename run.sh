#!/bin/bash

# Exit if any command fails
set -e

if [[ "$1" = "--skip-vagrant" ]]; then
    echo "Skip setting up vagrant VMs"
else
    vagrant up
fi

# Make sure our private key has correct permssions
chmod 600 ./vagrant_ssh_key

lein run -- test \
    --nodes-file nodes-vagrant.txt \
    --ssh-private-key ./vagrant_ssh_key \
    --username vagrant
