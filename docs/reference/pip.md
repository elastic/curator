---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/pip.html
---

# pip [pip]

This installation procedure requires a functional Python `pip` executable and requires that the target machine has internet connectivity for downloading Curator and the dependencies from [The Python Package Index](https://pypi.org).

```
pip install elasticsearch-curator
```

## Upgrading with pip [_upgrading_with_pip]

If you already have Elasticsearch Curator installed, and want to upgrade to the latest version, use the `-U` flag:

```
pip install -U elasticsearch-curator
```


## Installing a specific version with pip [_installing_a_specific_version_with_pip]

The `-U` flag uninstalls the current version (if any), then installs the latest version, or a specified one.  Specify a specific version by adding `==` followed by the version you’d like to install, like this:

```
pip install -U elasticsearch-curator==X.Y.Z
```

For example:

```sh subs=true
pip install -U elasticsearch-curator=={{version.curator}}
```

## System-wide vs. User-only installation [_system_wide_vs_user_only_installation]

The above commands each imply a system-wide installation.  This usually requires super-user access, or the `sudo` command.  There is a way to install Curator into a path for just the current user, using the `--user` flag.

```
pip install --user elasticsearch-curator
```

This will result in the `curator` end-point being installed in the current user’s home directory, in the `.local` directory, in the `bin` subdirectory. The full path might look something like this:

```
/home/user/.local/bin/curator
```

You can make an alias or a symlink to this so you can call it more easily.

The `--user` flag can also be used in conjunction with the `-U` flag:

```
pip install -U --user elasticsearch-curator==X.Y.Z
```