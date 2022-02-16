# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "debian/bullseye64"
  config.vm.synced_folder ".", "/vagrant", disabled: true

  # Jepsen can use this key to connect to our nodes
  config.vm.provision "shell" do |s|
    ssh_pub_key = File.readlines("vagrant_ssh_key.pub").first.strip
    s.inline = <<-SHELL
      echo #{ssh_pub_key} >> /home/vagrant/.ssh/authorized_keys
    SHELL
  end

  nodes = 2

  (1..nodes).each do |n|
    nid = "n#{n}"
    config.vm.define nid do |node|
      node.vm.hostname = nid
      node.vm.network "private_network", ip: "192.168.56.#{100 + n}"
    end
  end
end