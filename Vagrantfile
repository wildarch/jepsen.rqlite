# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "debian/bullseye64"
  config.vm.synced_folder ".", "/vagrant", disabled: true

  nodes = 1

  (1..nodes).each do |n|
    nid = "n#{n}"
    config.vm.define nid do |node|
      node.vm.hostname = nid
      node.vm.network "private_network", ip: "192.168.56.#{100 + n}"
    end
  end
end