#!/bin/bash
rm log.txt report.txt nohup* -f
python uploadanddownload.py
python uploadanddownload_rand.py

python uploadanddownload_dir.py

python lostpacket.py eth0 10%
python delaypacket.py eth0
