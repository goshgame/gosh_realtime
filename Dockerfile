FROM arm64v8/flink:1.20.0-scala_2.12

COPY plugins /opt/flink/plugins

COPY target/lib /opt/flink/lib

# 删除/opt/flink/lib目录下的S3相关jar包，这些jar包已经在plugins目录中
RUN rm -f /opt/flink/lib/aws-java-sdk-bundle-1.11.1030.jar \
    /opt/flink/lib/commons-cli-1.9.0.jar \
    /opt/flink/lib/flink-s3-fs-hadoop-1.20.0.jar \
    /opt/flink/lib/hadoop-aws-3.3.4.jar

COPY lib/dinky-app-1.20-1.2.4-jar-with-dependencies.jar /opt/flink


# dinky提交作业很坑，把所有日志文件都删了，只能曲线救国
RUN mkdir -p /opt/flink/conf_real \
    && cp /opt/flink/conf/* /opt/flink/conf_real/ \
    && chown -R flink:flink /opt/flink/conf_real \
    && rm -f /opt/flink/conf_real/config.yaml

RUN sed -i '/export FLINK_CONF_DIR/i cp /opt/flink/conf/config.yaml /opt/flink/conf_real' /opt/flink/bin/config.sh

RUN sed -i 's|export FLINK_CONF_DIR|export FLINK_CONF_DIR=/opt/flink/conf_real|' /opt/flink/bin/config.sh

RUN rm -rf ${FLINK_HOME}/lib/flink-table-planner-loader-*.jar