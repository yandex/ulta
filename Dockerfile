FROM cr.yandex/mirror/ubuntu:22.04 AS builder
ARG VERSION
ARG FEATURES=pandora,telegraf

RUN mkdir -p /etc/yandex-tank /var/lib/ulta /var/log/ulta
COPY build_files/yandextank_base_config/ /etc/yandex-tank/

COPY build_files/setup-deps.sh /bootstrap/
RUN chmod 0755 /bootstrap/setup-deps.sh
RUN /bootstrap/setup-deps.sh --features ${FEATURES}
COPY ulta /bootstrap/ulta/ulta
COPY setup.py /bootstrap/ulta/

RUN pip3 install /bootstrap/ulta/.
RUN mkdir /root/.ulta/

ENV GRPC_POLL_STRATEGY=poll

ENTRYPOINT ["ulta"]
