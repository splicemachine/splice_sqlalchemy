ARG splice_hadoop_image_version
FROM splicemachine/sm_k8_hdfs-3.0.0:$splice_hadoop_image_version

# Install HBase
RUN \
  yum install -y rlwrap && \
  mkdir $WORK_DIR && \
  cd $WORK_DIR && \ 
  wget $CDH_RPM_URL_86_64/hbase-2.1.0+$cdh_version.el7.x86_64.rpm && \ 
  yum install -y hbase-2.1.0+$cdh_version.el7.x86_64.rpm && \ 
  mkdir -p $HBASE_HOME/bin/splice && \
  mkdir -p $MLMANAGER_HOME/lib