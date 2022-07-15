FROM registry-vpc.cn-hangzhou.aliyuncs.com/docker-registry/jbt-data-collect-platform:base2
ADD jbt-data-collect-platform.tar.gz /data
RUN python -m pip install kafka-python && \
pip3 install -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com  kafka==1.3.5 
WORKDIR /data/jbt-data-collect-platform/data/ms/securities
