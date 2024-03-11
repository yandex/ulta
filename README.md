# Ulta loadtesting agent

The load testing agent for [Yandex Cloud Load Testing](https://cloud.yandex.ru/en/services/load-testing) performance analytics service.
It is intended to use as an [external agent](https://cloud.yandex.ru/en/docs/load-testing/tutorials/loadtesting-external-agent) in private networks, K8S, AWS, MS Azure and other clouds.

[//]: # (Add some picture here when Ulta is published to github)

## Quick start

Get the latest version from `cr.yandex` docker registry
```bash
docker pull cr.yandex/yc/ya-lt-agent:latest
```

or [build](#build-options) it locally

```bash
docker build .
```

The simple load test config may look like this:
```bash
# With this config Pandora performs `GET` requests to `172.16.0.12:443/path`
# and `172.16.0.12:443/another/path` URIs
# The load profile increases linearly from 1 to 100 requests per second 
# during 30 seconds, and then remains constant 100 RPS for 120 seconds.
#
# Autostop plugin will interrupt the test if the response time exceeds
# 15 seconds at 50% quantile.

$ echo '
autostop:
  enabled: true
  package: yandextank.plugins.Autostop
  autostop:
  - quantile (50,450ms,15s)
pandora:
  enabled: true
  package: yandextank.plugins.Pandora
  config_content:
    pools:
      - id: HTTP
        gun:
          type: http
          target: 172.16.0.12:443
          ssl: true
        ammo:
          type: uri
          uris:
            - /path
            - /another/path
        result:
          type: phout
          destination: ./phout.log
        startup:
          type: once
          times: 5000
        rps:
          - duration: 30s
            type: line
            from: 1
            to: 100
          - duration: 120s
            type: const
            ops: 100
    log:
      level: error
    monitoring:
      expvar:
        enabled: true
        port: 1234
' > load-test.yaml
```

Ulta uses [Yandex Tank](https://github.com/yandex/yandex-tank) under the hood to generate load, so you may run single test without the analytics service backend.

```bash
docker run --entrypoint yandex-tank -v load-test.yaml:/run/test.yaml \
           cr.yandex/yc/ya-lt-agent:latest -c /run/test.yaml
```


## Create and run your test with Load Testing analytics backend

To make it work you will need a [Yandex.Cloud account](https://cloud.yandex.ru/en/docs/billing/quickstart/) and optionally [yc cli](https://cloud.yandex.ru/en/docs/cli/quickstart).

First, create test in service

```bash
$ yc loadtesting config create --from-yaml-file load-test.yaml
id: ff67vbt6dmp5v83a7a8b
...

$ yc loadtesting test create --name sample-load-test --configuration id=ff67vbt6dmp5v83a7a8b
id: ff63y7kpe2me1wy0pty7
...

TEST_ID=ff63y7kpe2me1wy0pty7
```

If you have `jq` installed, the script may look like this:

```bash
CONFIG_ID=$(yc loadtesting config create --from-yaml-file load-test.yaml --format json | jq -r ".id")
TEST_ID=$(yc loadtesting test create --name sample-load-test --configuration id=$CONFIG_ID --format json | jq -r ".id")
```

Finally, execute your test:

```bash
LOADTESTING_YC_TOKEN=$(yc iam create-token)
FOLDER_ID=$(yc config get folder-id)

docker run --env LOADTESTING_YC_TOKEN=$LOADTESTING_YC_TOKEN \
       cr.yandex/yc/ya-lt-agent:latest run --folder-id $FOLDER_ID $TEST_ID
```

## Run as standalone agent

Ulta may run for a period of time, consuming tests from Load Testing service one by one, similar to [cloud load testing agents](https://cloud.yandex.ru/en/docs/load-testing/concepts/agent). In this mode `--agent-name` parameter is mandatory as service should recognize each long-living agent by name.

Consider using [Service Account key](#authorization-options) authorization in this mode.

```bash
docker run -v sakey.json:/run/sa.json cr.yandex/yc/ya-lt-agent:latest \
       --agent-name my-lt-agent --folder-id $FOLDER_ID \
       --sa-key-file /run/sa.json \
       serve
```


## Build options

By default Ulta comes with pandora and phantom load generators, and with telegraf binary to collect monitoring metrics from agent and target VMs.

You may build your own image using `--build-arg FEATURES=` arg: 

```bash
docker build . --build-arg FEATURES=jmeter,telegraf
```

Options supported:
* `pandora` - include pandora load generator binary
* `phantom` - include phantom load generator binary
* `jmeter` - include jmeter and some plugins
* `telegraf` - include telegraf binary


## Authorization options

Ulta supports 3 methods:

* [Service Account key](https://cloud.yandex.ru/en/docs/iam/concepts/authorization/key) - preferred method 
```bash
docker run --mount type=bind,from=./sa-key.json,to=/run/sa.json cr.yandex/yc/ya-lt-agent:latest --sa-key-file /run/sa.json --agent-name my-agent --folder-id $FOLDER_ID
```

* [IAM token](https://cloud.yandex.ru/en/docs/iam/concepts/authorization/iam-token) - has short lifetime period, so it works for short-term tasks
```bash
docker run --env LOADTESTING_YC_TOKEN=$(yc iam create-token) cr.yandex/yc/ya-lt-agent:latest  --agent-name my-agent --folder-id $FOLDER_ID
```

* [OAuth token](https://cloud.yandex.ru/en/docs/iam/concepts/authorization/oauth-token) - 1 year lifetime period
```bash
docker run --env LOADTESTING_OAUTH_TOKEN=$OAUTH_TOKEN cr.yandex/yc/ya-lt-agent:latest  --agent-name my-agent --folder-id $FOLDER_ID
```


## See also
- Please check official [documentation](https://cloud.yandex.ru/en/docs/load-testing/)

- See [pandora](https://yandex.github.io/pandora/) docs for more details about configuring load tests and test scenarios

- Available plugins for [Yandex.Tank](https://yandextank.readthedocs.io/en/latest/config_reference.html) 