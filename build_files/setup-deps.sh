#!/bin/bash

set -x -euo pipefail
DAEMON_NAME=docker-ulta

ULTA_PYTHON=${ULTA_PYTHON:-python3}

echo ">>> Python: $ULTA_PYTHON"

while [[ $# -gt 0 ]]; do
    case $1 in
    --features)
        features="$2"
        shift
        shift
        ;;
    *)
        shift
        ;;
    esac
done

export DEBIAN_FRONTEND=noninteractive
features=${features:="phantom,pandora,telegraf,jmeter"}

log_fatal() {
    logger -ip daemon.error -t ${DAEMON_NAME} "$@"
    exit 1
}

log_info() {
    logger -ip daemon.info -t ${DAEMON_NAME} "$@"
}

critical() {
    if ! "$@"; then
        log_fatal "Failed to execute $*"
    fi
}

stage() {
    log_info "### ENTER: $* ###"

    "$@"

    log_info "### EXIT: $* ###"
}

# Directory for temporary files
TMPDIR=$(mktemp -d)

system_upgrade() {
    # try to fix possible 'forbidden' errors with older cr.yandex/mirror/ubuntu 
    # in apt-get install
    rm -fr /var/lib/apt/lists/*
    DEBIAN_FRONTEND=noninteractive apt-get update -qq || log_fatal "Can't update repos"
    DEBIAN_FRONTEND=noninteractive LANG=C apt-get purge -y unattended-upgrades || log_fatal "Can't purge unattended-upgrades"
    DEBIAN_FRONTEND=noninteractive LANG=C apt-get install -y build-essential || log_fatal "Can't install build-essential"
    DEBIAN_FRONTEND=noninteractive LANG=C apt-get install -y locales || log_fatal "Can't install locales"
    locale-gen en_US.UTF-8 || log_fatal "Failed to set locales"
    dpkg-reconfigure --frontend=noninteractive locales || log_fatal "Can't rebuild locales"
    echo "* hard nofile 65536" >>/etc/security/limits.d/custom.conf
    echo "* soft nofile 65536" >>/etc/security/limits.d/custom.conf
}

install_python() {
    critical apt-get install --no-install-recommends -yq python3-pip
    critical python3 -m pip install --upgrade \
        "pip" \
        "setuptools>=51.0.0"
}

install_ulta_python() {
    critical apt-get install --no-install-recommends -yq \
        net-tools \
        gpg \
        gpg-agent \
        software-properties-common

    critical add-apt-repository ppa:deadsnakes/ppa
    critical apt-get install --no-install-recommends -yq \
        "$ULTA_PYTHON" \
        "$ULTA_PYTHON-dev" \
        "$ULTA_PYTHON-venv"
}

install_curl() {
    DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -yq curl || log_fatal "Can't install curl"
}

install_grpcurl() {
    GRPCURL_ARCHIVE="https://github.com/fullstorydev/grpcurl/releases/download/v1.8.2/grpcurl_1.8.2_linux_x86_64.tar.gz"
    curl -fS -L --connect-time 3 --max-time 30 --retry 10 "${GRPCURL_ARCHIVE}" -o ${TMPDIR}/grpcurl.tar.gz || log_fatal "Can't download grpcurl"
    pushd ${TMPDIR}
    tar -zxvf grpcurl.tar.gz || log_fatal "Can't extract grpcurl"
    cp ${TMPDIR}/grpcurl /usr/local/bin/grpcurl || log_fatal "Can't install grpcurl"
    popd
    ln -s /usr/local/bin/grpcurl /usr/bin/grpcurl
}

install_utils() {
    command -q curl || install_curl
    command -q grpcurl || install_grpcurl
}

install_phantom() {
    add-apt-repository ppa:yandex-load/main -y &&
        apt-get update -q &&
        apt-get install -yq \
            phantom=0.14.0~pre65load5nmu \
            phantom-ssl=0.14.0~pre65load5nmu
}

install_pandora() {
    PANDORA_BINARY="https://github.com/yandex/pandora/releases/download/v0.5.32/pandora_0.5.32_linux_amd64"
    curl -fS -L --connect-time 3 --max-time 30 --retry 10 "${PANDORA_BINARY}" -o /usr/local/bin/pandora || log_fatal "Can't download pandora"
    chmod +x /usr/local/bin/pandora
    mkdir -p /usr/share/doc/pandora
}

install_telegraf() {
    TELEGRAF_BINARY="https://dl.influxdata.com/telegraf/releases/telegraf_1.4.3-1_amd64.deb"
    curl -fS -L --connect-time 3 --max-time 30 --retry 10 "${TELEGRAF_BINARY}" -o ${TMPDIR}/telegraf.deb || log_fatal "Can't download telegraf"
    dpkg -i ${TMPDIR}/telegraf.deb
}

install_jmeter() {
    JMETER_VERSION="5.6.3"
    # tank jmeter plugin use version as float
    JMETER_VER=$(echo ${JMETER_VERSION} | sed -nr 's/^([0-9]+\.[0-9]+).*/\1/p')
    apt-get install -y openjdk-8-jdk
    curl -fS -L --connect-time 3 --retry 10 https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-${JMETER_VERSION}.tgz -o ${TMPDIR}/apache-jmeter.tgz || log_fatal "Can't download jmeter"
    tar zxvf ${TMPDIR}/apache-jmeter.tgz -C /usr/local/lib/ || log_fatal "Can't extract jmeter"

    JMETER_HOME="/usr/local/lib/apache-jmeter-${JMETER_VERSION}"
    ln -s ${JMETER_HOME}/bin/jmeter /usr/local/bin/jmeter

    echo "JVM_MAX_MEMORY=\$(free -bwl | awk '/Mem:/ { printf \"%d\", \$2 / 10 * 9}')" >${JMETER_HOME}/bin/setenv.sh
    echo 'JVM_ARGS="-Xms1g -Xmx${JVM_MAX_MEMORY}"' >>${JMETER_HOME}/bin/setenv.sh
    ln -s ${JMETER_HOME}/bin/setenv.sh /usr/local/bin/setenv.sh

    # install plugins
    JMETER_PLUGINS="jpgc-csl,jpgc-tst,jpgc-dummy,jmeter-jdbc,jpgc-functions,jpgc-casutg,bzm-http2"

    cd ${JMETER_HOME}/lib/ &&
        for lib in \
            "kg/apc/cmdrunner/2.3/cmdrunner-2.3.jar" \
            "org/postgresql/postgresql/42.1.4/postgresql-42.1.4.jar"; do
            local_name=$(echo "$lib" | awk -F'/' '{print $NF}')
            wget "https://search.maven.org/remotecontent?filepath=${lib}" -O "${local_name}" --progress=dot:mega
        done &&
        cd ${JMETER_HOME}/lib/ext
    wget 'https://search.maven.org/remotecontent?filepath=kg/apc/jmeter-plugins-manager/1.9/jmeter-plugins-manager-1.9.jar' -O jmeter-plugins-manager-1.9.jar --progress=dot:mega
    java -cp ${JMETER_HOME}/lib/ext/jmeter-plugins-manager-1.9.jar org.jmeterplugins.repository.PluginManagerCMDInstaller
    ${JMETER_HOME}/bin/PluginsManagerCMD.sh install "${JMETER_PLUGINS}"
    mkdir -p /etc/yandex-tank
    printf "jmeter:\n  jmeter_path: ${JMETER_HOME}/bin/jmeter\n  jmeter_ver: ${JMETER_VER}\n" >/etc/yandex-tank/10-jmeter.yaml
}

highload_twick() {
    echo "* hard nofile 65536" >>/etc/security/limits.conf
    echo "* soft nofile 65536" >>/etc/security/limits.conf
    echo "root hard nofile 65536" >>/etc/security/limits.conf
    echo "root soft nofile 65536" >>/etc/security/limits.conf
    echo "net.core.netdev_max_backlog=10000" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.core.somaxconn=262144" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_max_tw_buckets=720000" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_timestamps=1" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_tw_reuse=1" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_fin_timeout=30" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_keepalive_time=1800" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.core.rmem_max=26214400" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.core.rmem_default=6250000" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.core.wmem_max=26214400" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.core.wmem_default=6250000" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_rmem='4096 6250000 26214400'" >>/etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_wmem='4096 6250000 26214400'" >>/etc/sysctl.d/12-highload-network.conf
}

clean_apt() {
    DEBIAN_FRONTEND=noninteractive apt-get clean
}

cleanup() {
    rm -rf ${TMPDIR}
    rm -rf /var/log/apt/*
    rm -rf /var/log/{auth,dpkg,syslog,wtmp,kern,alternatives,auth,btmp,dmesg}.log
    rm -rf /var/lib/dhcp/*
    rm -rf /etc/apt/sources.list.d/yandex-load-*
    rm -rf /etc/apt/sources.list.d/deadsnakes-*
    rm -rf /etc/apt/trusted.gpg.d/yandex-load_ubuntu_main.gpg
    rm -rf /etc/network/interfaces.d/*
}

install_features() {
    IFS=','
    read -a fs <<<$features
    for f in "${fs[@]}"; do
        case $f in
        pandora)
            install_pandora
            ;;
        phantom)
            install_phantom
            ;;
        telegraf)
            install_telegraf
            ;;
        jmeter)
            install_jmeter
            ;;
        esac
    done
}

main() {
    stage system_upgrade
    stage install_python
    stage install_ulta_python
    stage install_utils
    stage install_features
    stage highload_twick
    stage clean_apt
    stage cleanup
}

main
