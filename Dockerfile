FROM registry-vpc.cn-hangzhou.aliyuncs.com/docker-registry/jbt-data-collect-platform:base
ADD jbt-data-collect-platform.tar.gz /data
WORKDIR /data/jbt-data-collect-platform/data/ms/securities