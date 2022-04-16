#!/bin/bash


echo "----- UPDATING APT AND APT-GET -----"
sudo apt update
sudo apt-get update
echo "----- DONE -----"


echo "----- INSTALLING CAMFLOW DEPENDENCIES -----"
sudo apt install -y build-essential 
sudo apt-get install -y libncurses5-dev libncursesw5-dev cmake clang wget git libssl-dev zlib1g patch mosquitto bison flex ruby dwarves libelf-dev uthash-dev libinih-devel uncrustify

git clone https://github.com/eclipse/paho.mqtt.c.git
cd paho.mqtt.c/
make
sudo make install
cd ..
echo "----- DONE -----"


echo "----- INSTALLING CAMFLOW -----"
git clone https://github.com/CamFlow/camflow-dev.git
cd camflow-dev/
git checkout dev
make prepare
make config-old # select relevant modules in security
make compile # patience, password will be asked during compilation
make install # patience, password will be asked during compilation

echo "----- DONE -----"

# sudo reboot now # reboot your machine
