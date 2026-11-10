FROM apache/hive:4.0.0

USER root
RUN mkdir -p /native-meta/warehouse /opt/native-hive \
    && chown -R hive:hive /native-meta /opt/native-hive
# Use the S3A and AWS SDK pair bundled with this image's Hadoop distribution.
RUN set -eu; \
    for pattern in hadoop-aws aws-java-sdk-bundle; do \
      set -- /opt/hadoop/share/hadoop/tools/lib/"$pattern"-*.jar; \
      test "$#" -eq 1; \
      test -f "$1"; \
      ln -s "$1" /opt/hive/lib/; \
    done
COPY hive-site.xml core-site.xml /opt/native-hive/
USER hive
