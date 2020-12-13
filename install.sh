cargo build
sudo systemctl stop collectd
read -p "Press [Enter] to start collectd with the teleport plugin..."
sudo cp target/debug/libteleport_collectd.so /usr/lib64/collectd/teleport_collectd.so
sudo systemctl start collectd
sudo systemctl status collectd

echo "Don't forget to allow network connections for plugins:"
echo \"setsebool collectd_tcp_network_connect on\"
