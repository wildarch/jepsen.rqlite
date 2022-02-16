#!/bin/bash

# Exit if any command fails
set -e

vagrant up

lein run -- test \
    --nodes-file nodes.txt \
    --ssh-private-key .vagrant/machines/n1/virtualbox/private_key \
    --username vagrant