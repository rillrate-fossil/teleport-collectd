systemctl stop collectd
cp target/debug/libteleport_collectd.so /usr/lib64/collectd/teleport_collectd.so
systemctl start collectd
systemctl status collectd
