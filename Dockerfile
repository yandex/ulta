FROM cr.yandex/mirror/ubuntu:22.04 AS builder
ARG VERSION
ARG FEATURES=pandora,telegraf

ARG PYTHON_VERSION=3.12
ENV ULTA_PYTHON="python${PYTHON_VERSION}"
ENV ULTA_VENV="/opt/ulta/venv"

RUN mkdir -p /etc/yandex-tank /var/lib/ulta /var/log/ulta
COPY build_files/yandextank_base_config/ /etc/yandex-tank/

COPY build_files/setup-deps.sh /bootstrap/
RUN chmod 0755 /bootstrap/setup-deps.sh
RUN /bootstrap/setup-deps.sh --features ${FEATURES}

ADD . /bootstrap/ulta

RUN /bootstrap/ulta/build_files/install-with-venv.sh \
    --build-constraint /bootstrap/ulta/build_constraints.txt /bootstrap/ulta

RUN rm -rf /bootstrap

ENV GRPC_POLL_STRATEGY=poll

RUN mkdir /root/.ulta/
ENTRYPOINT ["ulta"]
