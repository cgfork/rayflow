ARG BASE_IMAGE=ubuntu:24.04
FROM ${BASE_IMAGE}
COPY common.sh generate.sh /opt/rayflow/
ENTRYPOINT ["/bin/sh", "/opt/rayflow/generate.sh"]
