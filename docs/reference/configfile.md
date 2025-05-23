---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/configfile.html
---

# Configuration File [configfile]

::::{note}
The default location of the configuration file is `~/.curator/curator.yml`, but another location can be specified using the `--config` flag on the [command-line](/reference/command-line.md).
::::


::::{note}
You can use [environment variables](/reference/envvars.md) in your configuration files.
::::


The configuration file contains client connection and settings for logging.  It looks like this:

```sh
---
# Remember, leave a key empty if there is no value.  None will be a string,
# not a Python "NoneType"
elasticsearch:
  client:
    hosts:
      - http://127.0.0.1:9200
    cloud_id:
    ca_certs:
    client_cert:
    client_key:
    verify_certs:
    request_timeout: 30
  other_settings:
    master_only: False
    username:
    password:
    api_key:
      id:
      api_key:
      token:

logging:
  loglevel: INFO
  logfile:
  logformat: default
  blacklist: ['elastic_transport', 'urllib3']
```

It is a YAML configuration file.  The two root keys must be `elasticsearch` and `logging`.  The subkeys of each of these will be described here.

There are other keys available for the `client` subkey of the `elasticsearch` root key, many of which are listed [here](https://es-client.readthedocs.io/en/latest/defaults.html). The most commonly used ones (listed above) are described as follows:

## hosts [hosts]

::::{important}
All hosts must be in `HTTP[S]://FQDN.DOMAIN.TLD:PORT` form or they will be rejected and Curator will exit with an error. The only exception to this is `HTTPS://FQDN.DOMAIN.TLD` (without port), in which case `:443` is implicit, and is, in fact, appended automatically.
::::


::::{warning}
If both `cloud_id` and `hosts` keys are populated an exception will be thrown and Curator will exit.
::::


A `hosts` definition can be a single value:

```sh
hosts: http://127.0.0.1:9200
```

Or multiple values in the 3 acceptable YAML ways to render sequences, or arrays:

::::{warning}
Curator can only work with one cluster at a time.  Including clients from multiple clusters in the `hosts` setting will result in errors.
::::


Flow:

```sh
hosts: [ "http://10.0.0.1:9200", "http://10.0.0.2:9200" ]
```

Spanning:

```sh
hosts: [ "http://10.0.0.1:9200",
    "http://10.0.0.2:9200" ]
```

Block:

```sh
hosts:
  - http://10.0.0.1:9200
  - http://10.0.0.2:9200
```


## cloud_id [cloud_id]

The value should encapsulated in quotes because of the included colon:

```sh
cloud_id: 'deployment_name:BIG_HASH_VALUE'
```

::::{warning}
If both `cloud_id` and `hosts` keys are populated an exception will be thrown and Curator will exit.
::::



## ca_certs [ca_certs]

This should be a file path to your CA certificate, or left empty.

```sh
ca_certs:
```

This setting allows the use of a specified CA certificate file to validate the SSL certificate used by Elasticsearch.

There is no default.

::::{admonition} File paths
:class: tip

File paths can be specified as follows:

**For Windows:**

```sh
'C:\path\to\file'
```

**For Linux, BSD, Mac OS:**

```sh
'/path/to/file'
```

Using single-quotes around your file path is encouraged, especially with Windows file paths.

::::



## client_cert [client_cert]

This should be a file path to a client certificate (public key), or left empty.

```sh
client_cert:
```

Allows the use of a specified SSL client cert file to authenticate to Elasticsearch. The file may contain both an SSL client certificate and an SSL key, in which case [client_key](#client_key) is not used. If specifying `client_cert`, and the file specified does not also contain the key, use [client_key](#client_key) to specify the file containing the SSL key. The file must be in PEM format, and the key part, if used, must be an unencrypted key in PEM format as well.

::::{admonition} File paths
:class: tip

File paths can be specified as follows:

**For Windows:**

```sh
'C:\path\to\file'
```

**For Linux, BSD, Mac OS:**

```sh
'/path/to/file'
```

Using single-quotes around your file path is encouraged, especially with Windows file paths.

::::



## client_key [client_key]

This should be a file path to a client key (private key), or left empty.

```sh
client_key:
```

Allows the use of a specified SSL client key file to authenticate to Elasticsearch. If using [client_cert](#client_cert) and the file specified does not also contain the key, use `client_key` to specify the file containing the SSL key. The key file must be an unencrypted key in PEM format.

::::{admonition} File paths
:class: tip

File paths can be specified as follows:

**For Windows:**

```sh
'C:\path\to\file'
```

**For Linux, BSD, Mac OS:**

```sh
'/path/to/file'
```

Using single-quotes around your file path is encouraged, especially with Windows file paths.

::::



## verify_certs [verify_certs]

This should be `True`, `False` or left empty.

```sh
verify_certs:
```

If access to your Elasticsearch instance is protected by SSL encryption, you may set `verify_certs` to `False` to disable SSL certificate verification.

Valid use cases for doing so include the use of self-signed certificates that cannot be otherwise verified and would generate error messages.

::::{warning}
Setting `verify_certs` to `False` will likely result in a warning message that your SSL certificates are not trusted. This is expected behavior.
::::


The default value is `True`.


## request_timeout [request_timeout]

This should be an integer number of seconds, or left empty.

```sh
request_timeout:
```

You can change the default client connection timeout value with this setting.

The default value is `30` (seconds) should typically not be changed to be very large.  If a longer timeout is necessary for a given action, such as [snapshot](/reference/snapshot.md), [restore](/reference/restore.md), or [forcemerge](/reference/forcemerge.md), the client timeout can be overridden on per action basis by setting [timeout_override](/reference/option_timeout_override.md) in the action [options](/reference/options.md). There are default override values for some of those longer running actions.


## master_only [master_only]

This should be `True`, `False` or left empty.

```sh
master_only:
```

In some situations, primarily with automated deployments, it makes sense to install Curator on every node. But you wouldn’t want it to run on each node. By setting `master_only` to `True`, this is possible. It tests for, and will only continue running on the node that is the elected master.

::::{warning}
If `master_only` is `True`, and [hosts](#hosts) has more than one value, Curator will raise an Exception.  This setting should *only* be used with a single host in [hosts](#hosts), as its utility centers around deploying to all nodes in the cluster.
::::


The default value is `False`.


## username [username]

The HTTP Basic Authentication username


## password [password]

The HTTP Basic Authentication password


## id [id]

This should be the `id` portion of an API Key pair.

```sh
api_key:
  id:
```

This setting combined with the other subkey `api_key` allows API Key authentication to an Elasticsearch instance.

The default is empty.


## api_key [api_key]

This should be the `api_key` portion of an API Key pair.

```sh
api_key:
  api_key:
```

This setting combined with the other subkey `id` allows API Key authentication to an Elasticsearch instance.

The default is empty.


## token [token]

This should be a base64 encoded representation of an API Key pair.

```sh
api_key:
  token:
```

This setting will override any values provided for the `id` or `api_key` subkeys of `api_key`.

The default is empty.


## loglevel [loglevel]

This should be `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`, or left empty.

```sh
loglevel:
```

Set the minimum acceptable log severity to display.

* `CRITICAL` will only display critical messages.
* `ERROR` will only display error and critical messages.
* `WARNING` will display error, warning, and critical messages.
* `INFO` will display informational, error, warning, and critical messages.
* `DEBUG` will display debug messages, in addition to all of the above.

The default value is `INFO`.


## logfile [logfile]

This should be a path to a log file, or left empty.

```sh
logfile:
```

::::{admonition} File paths
:class: tip

File paths can be specified as follows:

**For Windows:**

```sh
'C:\path\to\file'
```

**For Linux, BSD, Mac OS:**

```sh
'/path/to/file'
```

Using single-quotes around your file path is encouraged, especially with Windows file paths.

::::


The default value is empty, which will result in logging to `STDOUT`, or the console.


## logformat [logformat]

This should `default`, `json`, `logstash`, `ecs` or left empty.

```sh
logformat:
```

The `default` format looks like:

```sh
2016-04-22 11:53:09,972 INFO      Action #1: ACTIONNAME
```

The `json` or `logstash` formats look like:

```sh
{"@timestamp": "2016-04-22T11:54:29.033Z", "function": "cli", "linenum": 178,
"loglevel": "INFO", "message": "Action #1: ACTIONNAME", "name": "curator.cli"}
```

The `ecs` format looks like:

```sh
{"@timestamp":"2020-02-22T11:55:00.022Z","log.level":"info","message":"Action #1:
ACTIONNAME","ecs":{"version":"1.6.0"},"log":{"logger":"curator.cli","origin":
{"file":{"line":178,"name":"cli.py"},"function":"run"},"original":"Action #1:
ACTIONNAME"},"process":{"name":"MainProcess","pid":12345,"thread":
{"id":123456789886543,"name":"MainThread"}}}
```

The default value is `default`.


## blacklist [blacklist]

This should be an empty array `[]`, an array of log handler strings, or left empty.

```sh
blacklist: ['elastic_transport', 'urllib3']
```

The default value is `['elastic_transport', 'urllib3']`, which will result in logs for the `elastic_transport` and `urllib3` Python modules *not* being output. These can be quite verbose, so unless you need them to debug an issue, you should accept the default value.

::::{tip}
If you do need to troubleshoot an issue, set `blacklist` to `[]`, which is an empty array.  Leaving it unset will result in the default behavior, which is to filter out `elastic_transport` and `urllib3` log traffic.
::::



