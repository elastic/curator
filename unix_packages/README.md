# README

## Docker Package Building for Linux Systems

The image is based on CentOS 6 (yes, I know it's EOL--this preserves it
for the people still using it, and the resulting binary runs nearly everywhere due 
to it's ancient LIBC version).

### Update/edit the image

If you edit the `Dockerfile`, you will see:

```
# Can change these
ENV PYVER=3.9
ENV PYPATCH=4
ENV OPENSSL_VER=1.1.1k
```

You should be able to edit these values with whatever is available at the respective
sites, python.org and openssl.org.

### Build the image
To build the image, `cd` to the `builder_image` directory and run:

```
docker build -t curator_builder:latest .
```

### Build packages

The following will check out the tag identified by `tags/vX.Y.Z` from the Curator
Github repository and build package which will be deposited in the present working directory.

```
docker run --rm -v $(pwd):/curator_packages curator_builder /package_maker.sh X.Y.Z
```

The result will be an RPM package named:

```
elasticsearch-curator-X.Y.Z-1.x86_64.rpm
```

and a DEB package named:

```
elasticsearch-curator_X.Y.Z_amd64.deb
```

These packages were tested in CentOS 6 & 7; Ubuntu 1404, 1604, and 1804; and Debian 8 & 9.

## Publishing of Packages

This process is used to create packages which are subsequently signed with Elastic's key
and published to the repositories identified in the Curator documentation.
