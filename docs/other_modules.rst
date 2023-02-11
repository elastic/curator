.. _other_modules:

Other Modules
#############

``curator.cli``
===============

.. py:module:: curator.cli

.. autofunction:: process_action

.. autofunction:: run

.. py:function:: cli(config, dry_run, action_file)

    This is the :py:class:`click.Command` that initiates everything and connects the command-line to
    the rest of Curator.

    :param config: Path to configuration file. Default: ~/.curator/curator.yml
    :param dry_run: Do not perform any changes.
    :param action_file: Path to action file

    :type config: str
    :type dry_run: bool
    :type action_file: str

``curator.config_utils``
========================

.. py:module:: curator.config_utils

.. autofunction:: check_logging_config

.. autofunction:: set_logging

.. autofunction:: password_filter

``curator.logtools``
====================

.. py:module:: curator.logtools

.. autofunction:: de_dot

.. autofunction:: deepmerge

.. autoclass:: LogInfo

``curator.logtools.LogstashFormatter``
--------------------------------------

This inherits from :py:class:`logging.Formatter`, so some of what you see documented is inherited.

.. autoclass:: LogstashFormatter

``curator.repomgrcli``
======================

.. py:module:: curator.repomgrcli

.. autofunction:: delete_callback

.. autofunction:: show_repos

.. autofunction:: get_client

.. autofunction:: create_repo

.. py:function:: azure(ctx, name, client, container, base_path, chunk_size, compress, max_restore_rate, max_snapshot_rate, readonly, location_mode, verify)

    :param ctx: The Click Context
    :param name: The repository name
    :param client: The named client (Azure)
    :param container: Container name. You must create the Azure container before creating the
        repository.
    :param base_path: Specifies the path within container to repository data. Defaults to empty
        (root directory).
    :param chunk_size: Chunk size, e.g. ``1g``, ``10m``, ``5k``. Default is unbounded.
    :param compress: Enable/Disable metadata compression.
    :param max_restore_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param max_snapshot_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param readonly: Make repsitory read-only.
    :param location_mode: Either ``primary_only`` or ``secondary_only``. Note that if you set it to
        ``secondary_only``, it will force ``readonly`` to ``True``.
    :param verify: Verify repository after creation.

    :type ctx: :py:class:`~.click.Context`
    :type name: str
    :type client: str
    :type container: str
    :type base_path: str
    :type chunk_size: str
    :type compress: bool
    :type max_restore_rate: str
    :type max_snapshot_rate: str
    :type readonly: bool
    :type location_mode: str
    :type verify: bool

.. py:function:: gcs(ctx, name, bucket, client, base_path, chunk_size, compress, max_restore_rate, max_snapshot_rate, readonly, verify)

    :param ctx: The Click Context
    :param name: The repository name
    :param client: The name of the client to use to connect to Google Cloud Storage.
    :param bucket: The name of the bucket to be used for snapshots.
    :param base_path: Specifies the path within bucket to repository data. Defaults to the root of
        the bucket.
    :param chunk_size: Chunk size, e.g. ``1g``, ``10m``, ``5k``. Default is unbounded.
    :param compress: Enable/Disable metadata compression.
    :param max_restore_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param max_snapshot_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param readonly: Make repsitory read-only.
    :param verify: Verify repository after creation.

    :type ctx: :py:class:`~.click.Context`
    :type name: str
    :type client: str
    :type bucket: str
    :type base_path: str
    :type chunk_size: str
    :type compress: bool
    :type max_restore_rate: str
    :type max_snapshot_rate: str
    :type readonly: bool
    :type verify: bool

.. py:function:: s3(ctx, name, bucket, client, base_path, chunk_size, compress, max_restore_rate, max_snapshot_rate, readonly, server_side_encryption, buffer_size, canned_acl, storage_class, verify)

    :param ctx: The Click Context
    :param name: The repository name
    :param bucket: The bucket name must adhere to Amazon's S3 bucket naming rules.
    :param client: The name of the S3 client to use to connect to S3.
    :param base_path: Specifies the path to the repository data within its bucket. Defaults to an
        empty string, meaning that the repository is at the root of the bucket. The value of this
        setting should not start or end with a /.
    :param chunk_size: Chunk size, e.g. ``1g``, ``10m``, ``5k``. Default is unbounded.
    :param compress: Enable/Disable metadata compression.
    :param max_restore_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param max_snapshot_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param readonly: Make repsitory read-only.
    :param server_side_encryption: If set, files are encrypted on server side using AES256 algorithm.
    :param buffer_size: Minimum threshold below which the chunk is uploaded using a single request.
        Must be between 5mb and 5gb.
    :param canned_acl: When the S3 repository creates buckets and objects, it adds the canned ACL
        into the buckets and objects.
    :param storage_class: Sets the S3 storage class for objects stored in the snapshot repository.
    :param verify: Verify repository after creation.

    :type ctx: :py:class:`~.click.Context`
    :type name: str
    :type bucket: str
    :type client: str
    :type base_path: str
    :type chunk_size: str
    :type compress: bool
    :type max_restore_rate: str
    :type max_snapshot_rate: str
    :type readonly: bool
    :type server_side_encryption: bool
    :type buffer_size: str
    :type canned_acl: str
    :type storage_class: str
    :type verify: bool

.. py:function:: fs(ctx, name, location, compress, chunk_size, max_snapshots, max_restore_rate, max_snapshot_rate, readonly, verify)

    :param ctx: The Click Context
    :param name: The repository name
    :param location: Shared file-system location. Must match remote path, & be accessible to all
        master & data nodes
    :param compress: Enable/Disable metadata compression.
    :param chunk_size: Chunk size, e.g. ``1g``, ``10m``, ``5k``. Default is unbounded.
    :param max_snapshots: Maximum number of snapshots the repository can contain. Defaults to
        ``Integer.MAX_VALUE``, which is 2147483647.
    :param max_restore_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param max_snapshot_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param readonly: Make repsitory read-only.
    :param verify: Verify repository after creation.

    :type ctx: :py:class:`~.click.Context`
    :type name: str
    :type location: str
    :type compress: bool
    :type chunk_size: str
    :type max_snapshots: int
    :type max_restore_rate: str
    :type max_snapshot_rate: str
    :type readonly: bool
    :type verify: bool

.. py:function:: url(ctx, name, chunk_size, http_max_retries, http_socket_timeout, compress, max_snapshots, max_restore_rate, shared_filesystem_url, verify)

    :param ctx: The Click Context
    :param name: The repository name
    :param chunk_size: Chunk size, e.g. ``1g``, ``10m``, ``5k``. Default is unbounded.
    :param http_max_retries: Maximum number of retries for http and https
    :param http_socket_timeout: Maximum wait time for data transfers over a connection.
    :param compress: Enable/Disable metadata compression.
    :param max_snapshots: Maximum number of snapshots the repository can contain. Defaults to
        ``Integer.MAX_VALUE``, which is 2147483647.
    :param max_restore_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param shared_filesystem_url: URL location of the root of the shared filesystem repository.
    :param verify: Verify repository after creation.

    :type ctx: :py:class:`~.click.Context`
    :type name: str
    :type chunk_size: str
    :type http_max_retries: int
    :type http_socket_timeout: int
    :type compress: bool
    :type max_snapshots: int
    :type max_restore_rate: str
    :type shared_filesystem_url: str
    :type verify: bool

.. py:function:: source(ctx, name, delegate_type, location, compress, chunk_size, max_snapshots, max_restore_rate, max_snapshot_rate, readonly, verify)

    :param ctx: The Click Context
    :param name: The repository name
    :param delegate_type: Delegated repository type.
    :param location: Shared file-system location. Must match remote path, & be accessible to all
        master & data nodes
    :param compress: Enable/Disable metadata compression.
    :param chunk_size: Chunk size, e.g. ``1g``, ``10m``, ``5k``. Default is unbounded.
    :param max_snapshots: Maximum number of snapshots the repository can contain. Defaults to
        ``Integer.MAX_VALUE``, which is 2147483647.
    :param max_restore_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param max_snapshot_rate: Throttles per node restore rate (per second). Default is ``20mb``.
    :param readonly: Make repsitory read-only.
    :param verify: Verify repository after creation.

    :type ctx: :py:class:`~.click.Context`
    :type name: str
    :type delegate_type: str
    :type location: str
    :type compress: bool
    :type chunk_size: str
    :type max_snapshots: int
    :type max_restore_rate: str
    :type max_snapshot_rate: str
    :type readonly: bool
    :type verify: bool

.. py:function:: repo_mgr_cli(ctx, config, hosts, cloud_id, id, api_key, username, password, bearer_auth, opaque_id, request_timeout, http_compress, verify_certs, ca_certs, client_cert, client_key, ssl_assert_hostname, ssl_assert_fingerprint, ssl_version, master_only, skip_version_test, dry_run, loglevel, logfile, logformat)

    :param ctx: The Click Context
    :param config: Path to configuration file.
    :param hosts: Elasticsearch URL to connect to
    :param cloud_id: Shorthand to connect to Elastic Cloud instance
    :param id: API Key "id" value
    :param api_key: API Key "api_key" value
    :param username: Username used to create "basic_auth" tuple
    :param password: Password used to create "basic_auth" tuple
    :param bearer_auth: Bearer Auth Token
    :param opaque_id: Opaque ID string
    :param request_timeout: Request timeout in seconds
    :param http_compress: Enable HTTP compression
    :param verify_certs: Verify SSL/TLS certificate(s)
    :param ca_certs: Path to CA certificate file or directory
    :param client_cert: Path to client certificate file
    :param client_key: Path to client certificate key
    :param ssl_assert_hostname: Hostname or IP address to verify on the node's certificate.
    :param ssl_assert_fingerprint: SHA-256 fingerprint of the node's certificate. If this value is
        given then root-of-trust verification isn't done and only the node's certificate
        fingerprint is verified.
    :param ssl_version: Minimum acceptable TLS/SSL version
    :param master_only: Only run if the single host provided is the elected master
    :param skip_version_test: Do not check the host version
    :param dry_run: Do not perform any changes.
    :param loglevel: Log level
    :param logfile: Path to log file
    :param logformat: Log output format

    :type ctx: :py:class:`~.click.Context`
    :type config: str
    :type hosts: list
    :type cloud_id: str
    :type id: str
    :type api_key: str
    :type username: str
    :type password: str
    :type bearer_auth: str
    :type opaque_id: str
    :type request_timeout: int
    :type http_compress: bool
    :type verify_certs: bool
    :type ca_certs: str
    :type client_cert: str
    :type client_key: str
    :type ssl_assert_hostname: str
    :type ssl_assert_fingerprint: str
    :type ssl_version: str
    :type master_only: bool
    :type skip_version_test: bool
    :type dry_run: bool
    :type loglevel: str
    :type logfile: str
    :type logformat: str
