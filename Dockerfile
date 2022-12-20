FROM registry-vpc.cn-hangzhou.aliyuncs.com/docker-registry/jbt-data-collect-platform:base3
ADD jbt-data-collect-platform.tar.gz /data
RUN pip3 install -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com lxml xlrd openpyxl html5lib==1.1
#RUN pip3 install -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com  kafka==1.3.5 && \
#pip3 uninstall kafka-python && \
#python -m pip install kafka-python
WORKDIR /data/jbt-data-collect-platform/data/ms/securities
