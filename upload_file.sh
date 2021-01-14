#!/bin/bash
rm -rf /yts3/s3cache  #yts3缓存目录，根据配置修改
while true
do
  python upload_consistent.py
done