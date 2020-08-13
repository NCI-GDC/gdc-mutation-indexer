# -*- mode: ruby -*-
# vi: set ft=ruby :


$core_setup = <<-SCRIPT
  apt-get update
  apt-get install -y openjdk-8-jdk python-pip unzip
  pip install virtualenv
SCRIPT


$es_setup = <<-SCRIPT
if [ ! -f elasticsearch-${ES_VERSION}-amd64.deb ]
then
  curl -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${ES_VERSION}-amd64.deb && sudo dpkg --force-confnew -i elasticsearch-${ES_VERSION}-amd64.deb
  sudo /usr/share/elasticsearch/bin/elasticsearch-plugin install mapper-size
  sudo sed -i 's/^-Xms.*/-Xms2g/' /etc/elasticsearch/jvm.options
  sudo sed -i 's/^-Xmx.*/-Xmx2g/' /etc/elasticsearch/jvm.options
fi

if [ ! -f /mnt/swap.file ]
then
  sudo fallocate -l 5G /mnt/swap.file
  sudo mkswap /mnt/swap.file
  sudo swapon /mnt/swap.file
fi

sudo chown elasticsearch /etc/default/elasticsearch
sudo service elasticsearch restart
SCRIPT


$spark_setup = <<-SCRIPT
if [ ! -d spark-${SPARK_VERSION}-bin-hadoop2.7 ]
then
  wget --progress=bar:force https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
  tar -xzvf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
fi

if [ ! -d elasticsearch-hadoop-${ES_VERSION} ]
then
  wget --progress=bar:force https://artifacts.elastic.co/downloads/elasticsearch-hadoop/elasticsearch-hadoop-${ES_VERSION}.zip
  unzip elasticsearch-hadoop-${ES_VERSION}.zip
fi

cp /home/vagrant/elasticsearch-hadoop-${ES_VERSION}/dist/elasticsearch-spark-20_2.11-${ES_VERSION}.jar $SPARK_HOME/jars
SCRIPT


$setup_dev_environment = <<-SCRIPT
if [ -d /home/vagrant/venv ]
then
  rm -rf /home/vagrant/venv
fi

virtualenv /home/vagrant/venv
source /home/vagrant/venv/bin/activate
ssh-keyscan github.com >> /home/vagrant/.ssh/known_hosts
cd /vagrant
pip install -r requirements.txt
pip install -r dev-requirements.txt
python setup.py develop
SCRIPT


# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure("2") do |config|
  # The most common configuration options are documented and commented below.
  # For a complete reference, please see the online documentation at
  # https://docs.vagrantup.com.

  # Every Vagrant development environment requires a box. You can search for
  # boxes at https://vagrantcloud.com/search.
  config.vm.box = "hashicorp/bionic64"

  # Give some extra RAM juice
  config.vm.provider "virtualbox" do |vb|
    vb.memory = 4096
    vb.cpus = 2
  end

  # Forward ssh agent
  config.ssh.forward_agent = true

  # Enable provisioning with a shell script. Additional provisioners such as
  # Ansible, Chef, Docker, Puppet and Salt are also available. Please see the
  # documentation for more information about their specific syntax and use.

  # Pre setup environment variables
  config.vm.provision "load test environment", type: "shell",
    inline: "echo 'source /vagrant/tests/vagrant-test-env.sh' > /etc/profile.d/sa-environment.sh",
      :run => 'always'

  # Install Java8, pip, virtualenv
  config.vm.provision "install core libs", type: "shell", inline: $core_setup

  # Do ES setup
  config.vm.provision "setup elasticsearch", type: "shell", inline: $es_setup, privileged: false

  # Get Spark
  config.vm.provision "setup pyspark", type: "shell", inline: $spark_setup, privileged: false

  # Virtualenv setup
  config.vm.provision "setup dev-environment", type: "shell", inline: $setup_dev_environment, privileged: false

end
