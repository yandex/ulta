#!/bin/bash

set -x -euo pipefail
DAEMON_NAME=docker-ulta

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

features=${features:="phantom,pandora,telegraf,jmeter"}

log_fatal (){
    logger -ip daemon.error -t ${DAEMON_NAME} "$@"
    exit 1
}

log_info (){
    logger -ip daemon.info -t ${DAEMON_NAME} "$@"
}

# Directory for temporary files
TMPDIR=$(mktemp -d)

system_upgrade() {
    DEBIAN_FRONTEND=noninteractive apt-get update -qq || log_fatal "Can't update repos"
    DEBIAN_FRONTEND=noninteractive LANG=C apt-get purge -y unattended-upgrades || log_fatal "Can't purge unattended-upgrades"
    DEBIAN_FRONTEND=noninteractive LANG=C apt-get install -y locales || log_fatal "Can't install locales"
    locale-gen en_US.UTF-8 || log_fatal "Failed to set locales"
    dpkg-reconfigure --frontend=noninteractive locales || log_fatal "Can't rebuild locales"
    echo "* hard nofile 65536" >> /etc/security/limits.d/custom.conf
    echo "* soft nofile 65536" >> /etc/security/limits.d/custom.conf
}

install_utils() {
    DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -yq net-tools gpg gpg-agent \
        python3.11 \
        python3-pip \
        python3-venv || log_fatal "Can't install utils"
    pip3 install 'setuptools>=51.0.0'
}

install_curl() {
    DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -yq curl || log_fatal "Can't install curl"
}

install_phantom() {
    DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -yq software-properties-common || log_fatal "Can't install software-properties-common"
    add-apt-repository ppa:yandex-load/main -y && \
    apt-get update -q && \
    apt-get install -yq \
        phantom=0.14.0~pre65load5nmu \
        phantom-ssl=0.14.0~pre65load5nmu
}

install_pandora() {
    install_curl
    PANDORA_BINARY="https://github.com/yandex/pandora/releases/download/v0.5.25/pandora_0.5.25_linux_amd64"
    curl -fS -L --connect-time 3 --max-time 30 --retry 10 "${PANDORA_BINARY}" -o /usr/local/bin/pandora || log_fatal "Can't download pandora"
    chmod +x /usr/local/bin/pandora
    mkdir -p /usr/share/doc/pandora
}

install_telegraf() {
    install_curl
    TELEGRAF_BINARY="https://dl.influxdata.com/telegraf/releases/telegraf_1.4.3-1_amd64.deb"
    curl -fS -L --connect-time 3 --max-time 30 --retry 10 "${TELEGRAF_BINARY}" -o ${TMPDIR}/telegraf.deb || log_fatal "Can't download telegraf"
    dpkg -i ${TMPDIR}/telegraf.deb
}

install_jmeter() {
    JMETER_VERSION="5.5"
    apt-get install -y openjdk-8-jdk
    curl -fS -L --connect-time 3 --retry 10 https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-${JMETER_VERSION}.tgz -o ${TMPDIR}/apache-jmeter.tgz
    tar zxvf ${TMPDIR}/apache-jmeter.tgz -C /usr/local/lib/

    JMETER_HOME="/usr/local/lib/apache-jmeter-${JMETER_VERSION}"
    ln -s ${JMETER_HOME}/bin/jmeter /usr/local/bin/jmeter

    # install plugins
    JMETER_PLUGINS="jpgc-csl,jpgc-tst,jpgc-dummy,jmeter-jdbc,jpgc-functions,jpgc-casutg,bzm-http2"

    cd ${JMETER_HOME}/lib/ && \
    for lib in \
        "kg/apc/cmdrunner/2.3/cmdrunner-2.3.jar" \
        "org/postgresql/postgresql/42.1.4/postgresql-42.1.4.jar"; \
    do local_name=$(echo "$lib" | awk -F'/' '{print $NF}') ; \
        wget "https://search.maven.org/remotecontent?filepath=${lib}" -O "${local_name}" --progress=dot:mega ;\
    done && \
    cd ${JMETER_HOME}/lib/ext
    wget 'https://search.maven.org/remotecontent?filepath=kg/apc/jmeter-plugins-manager/1.9/jmeter-plugins-manager-1.9.jar' -O jmeter-plugins-manager-1.9.jar --progress=dot:mega
    java -cp ${JMETER_HOME}/lib/ext/jmeter-plugins-manager-1.9.jar org.jmeterplugins.repository.PluginManagerCMDInstaller
    ${JMETER_HOME}/bin/PluginsManagerCMD.sh install "${JMETER_PLUGINS}"
    mkdir -p /etc/yandex-tank
    printf "jmeter:\n  jmeter_path: ${JMETER_HOME}/bin/jmeter\n  jmeter_ver: ${JMETER_VERSION}\n" > /etc/yandex-tank/10-jmeter.yaml

}

highload_twick() {
    echo "* hard nofile 65536" >> /etc/security/limits.conf
    echo "* soft nofile 65536" >> /etc/security/limits.conf
    echo "root hard nofile 65536" >> /etc/security/limits.conf
    echo "root soft nofile 65536" >> /etc/security/limits.conf
    echo "net.core.netdev_max_backlog=10000" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.core.somaxconn=262144" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_max_tw_buckets=720000" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_timestamps=1" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_tw_reuse=1" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_fin_timeout=30" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_keepalive_time=1800" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.core.rmem_max=26214400" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.core.rmem_default=6250000" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.core.wmem_max=26214400" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.core.wmem_default=6250000" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_rmem='4096 6250000 26214400'" >> /etc/sysctl.d/12-highload-network.conf
    echo "net.ipv4.tcp_wmem='4096 6250000 26214400'" >> /etc/sysctl.d/12-highload-network.conf
}

clean_apt() {
    DEBIAN_FRONTEND=noninteractive apt-get clean
}

cleanup () {
    rm -rf ${TMPDIR}
    rm -rf /var/log/apt/*
    rm -rf /var/log/{auth,dpkg,syslog,wtmp,kern,alternatives,auth,btmp,dmesg}.log
    rm -rf /var/lib/dhcp/*
    rm -rf /etc/apt/sources.list.d/yandex-load-ubuntu-main-focal.list
    rm -rf /etc/apt/trusted.gpg.d/yandex-load_ubuntu_main.gpg
    rm -rf /etc/network/interfaces.d/*
}

install_features() {
    IFS=','
    read -a fs <<< $features
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
    system_upgrade
    install_utils
    install_features
    highload_twick
    clean_apt
    cleanup
}

main
