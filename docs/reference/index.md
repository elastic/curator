---
navigation_title: "Curator"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/versions.html
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/current_release.html
---

# Curator index management

This section provides reference information for Elasticsearch Curator.

## Versions [versions]

Elasticsearch Curator has been around for many different versions of {{es}}. Earlier releases of Curator supported multiple versions of {{es}}, but this is no longer the case.

Curator is now major version locked with {{es}}, which means that if Curator’s major version is 8, it should support any {{es}} 8.x release.

### Current release [current_release]

The current version of Curator {{curator_major}} is {{version.curator}}. 

### Running Curator {{curator_major}} with {{es}} {{version.stack | M}} [running-curator-8-with-stack-9]

Because Curator {{curator_major}} and {{es}} {{version.stack | M}} have a different major version, Curator will fail to run against an {{es}} {{version.stack | M}} cluster:

```sh subs=true
ERROR     Elasticsearch version {{version.stack}} not supported
CRITICAL  Unable to establish client connection to Elasticsearch!
CRITICAL  Exception encountered: Elasticsearch version {{version.stack}} not supported
```

To run {{curator_major}} with {{es}} {{version.stack | M}}, pass the `--skip_version_test` flag when running [`curator_cli`](./singleton-cli.md).