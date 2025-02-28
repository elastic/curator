---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/command-line.html
---

# Command Line Interface [command-line]

Most common client configuration settings are now available at the command-line.

::::{important}
While both the configuration file and the command-line arguments can be used together, it is important to note that command-line options will override file-based configuration of the same setting.
::::


The most basic command-line arguments are as follows:

```sh
curator [--config CONFIG.YML] [--dry-run] ACTION_FILE.YML
```

The square braces indicate optional elements.

If `--config` and `CONFIG.YML` are not provided, Curator will look in `~/.curator/curator.yml` for the configuration file.  `~` is the home directory of the user executing Curator. In a Unix system, this might be `/home/username/.curator/curator.yml`, while on a Windows system, it might be `C:\Users\username\.curator\curator.yml`

If `--dry-run` is included, Curator will simulate the action(s) in ACTION_FILE.YML as closely as possible without actually making any changes.  The results will be in the logfile, or STDOUT/command-line if no logfile is specified.

`ACTION_FILE.YML` is a YAML [actionfile](/reference/actionfile.md).

For other client configuration options, command-line help is never far away:

```sh
curator --help
```

The help output looks like this:

```sh
$ curator --help
Usage: curator [OPTIONS] ACTION_FILE

  Curator for Elasticsearch indices

  The default $HOME/.curator/curator.yml configuration file (--config) can be used but is not needed.

  Command-line settings will always override YAML configuration settings.

  Some less-frequently used client configuration options are now hidden. To see the full list, run:

      curator_cli -h

Options:
  --config PATH                   Path to configuration file.
  --hosts TEXT                    Elasticsearch URL to connect to.
  --cloud_id TEXT                 Elastic Cloud instance id
  --api_token TEXT                The base64 encoded API Key token
  --id TEXT                       API Key "id" value
  --api_key TEXT                  API Key "api_key" value
  --username TEXT                 Elasticsearch username
  --password TEXT                 Elasticsearch password
  --request_timeout FLOAT         Request timeout in seconds
  --verify_certs / --no-verify_certs
                                  Verify SSL/TLS certificate(s)  [default: verify_certs]
  --ca_certs TEXT                 Path to CA certificate file or directory
  --client_cert TEXT              Path to client certificate file
  --client_key TEXT               Path to client key file
  --dry-run                       Do not perform any changes.
  --loglevel [DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                  Log level
  --logfile TEXT                  Log file
  --logformat [default|ecs]       Log output format
  -v, --version                   Show the version and exit.
  -h, --help                      Show this message and exit.

  Learn more at https://www.elastic.co/guide/en/elasticsearch/client/curator/8.0/command-line.html
```

You can use [environment variables](/reference/envvars.md) in your configuration files.

## Running Curator from Docker [_running_curator_from_docker]

Running Curator from the command-line using Docker requires only a few additional steps.

Should you desire to use them, Docker-based Curator requires you to map a volume for your configuration and/or log files. Attempting to read a YAML configuration file if you have neglected to volume map your configuration directory to `/.curator` will not work.

It looks like this:

```sh
docker run [-t] --rm --name myimagename \
  -v /PATH/TO/MY/CONFIGS:/.curator      \
  untergeek/curator:mytag               \
--config /.curator/config.yml /.curator/actionfile.yml
```

::::{note}
While testing, adding the `-t` flag will allocate a pseudo-tty, allowing you to see terminal output that would otherwise be hidden.
::::


Both of the files `config.yml` and `actionfile.yml` should already exist in the path `/PATH/TO/MY/CONFIGS` before run time.

The `--rm` in the command means that the container (not the image) will be deleted after completing execution. You definitely want this as there is no reason to keep creating containers for each run. The eventual cleanup from this would be unpleasant.

 


