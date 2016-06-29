# Contributing to Curator

All contributions are welcome: ideas, patches, documentation, bug reports,
complaints, etc!

Programming is not a required skill, and there are many ways to help out!
It is more important to us that you are able to contribute.

That said, some basic guidelines, which you are free to ignore :)

## Want to learn?

Want to write your own code to do something Curator doesn't do out of the box?

* [Curator API Documentation](http://curator.readthedocs.io/) Since version 2.0,
Curator ships with both an API and wrapper scripts (which are actually defined
as entry points).  This allows you to write your own scripts to accomplish
similar goals, or even new and different things with the
[Curator API](http://curator.readthedocs.io/), and the
[Elasticsearch Python API](http://elasticsearch-py.readthedocs.io/).

Want to know how to use the command-line interface (CLI)?

* [Curator CLI Documentation](http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html)
  The Curator CLI Documentation is now a part of the document repository at
  http://elastic.co/guide at
  http://www.elastic.co/guide/en/elasticsearch/client/curator/current/index.html

Want to lurk about and see what others are doing with Curator?

* The irc channels (#logstash and #elasticsearch on irc.freenode.org) are good
  places for this

## Got Questions?

Have a problem you want Curator to solve for you?

* You are welcome to join the IRC channel #logstash (or #elasticsearch) on
irc.freenode.org and ask for help there!

## Have an Idea or Feature Request?

* File a ticket on [github](https://github.com/elastic/curator/issues)

## Something Not Working? Found a Bug?

If you think you found a bug, it probably is a bug.

* File it on [github](https://github.com/elastic/curator/issues)

# Contributing Documentation and Code Changes

If you have a bugfix or new feature that you would like to contribute to
Curator, and you think it will take more than a few minutes to produce the fix
(ie; write code), it is worth discussing the change with the Curator users and
developers first! You can reach us via
[github](https://github.com/elastic/curator/issues), or via IRC (#logstash or
 #elasticsearch on freenode irc)

Documentation is in two parts: API and CLI documentation.

API documentation is generated from comments inside the classes and methods
within the code.  This documentation is rendered and hosted at
http://curator.readthedocs.io

CLI documentation is in Asciidoc format in the GitHub repository at
https://github.com/elastic/curator/tree/master/docs/asciidoc.
This documentation can be changed via a pull request as with any other code
change.

## Contribution Steps

1. Test your changes! Run the test suite ('python setup.py test').  Please note
   that this requires an Elasticsearch instance. The tests will try to connect
   to your local elasticsearch instance and run integration tests against it.
   **This will delete all the data stored there!** You can use the env variable
   `TEST_ES_SERVER` to point to a different instance (for example
   'otherhost:9203').
2. Please make sure you have signed our [Contributor License
   Agreement](http://www.elastic.co/contributor-agreement/). We are not
   asking you to assign copyright to us, but to give us the right to distribute
   your code without restriction. We ask this of all contributors in order to
   assure our users of the origin and continuing existence of the code. You
   only need to sign the CLA once.
3. Send a pull request! Push your changes to your fork of the repository and
   [submit a pull
   request](https://help.github.com/articles/using-pull-requests). In the pull
   request, describe what your changes do and mention any bugs/issues related
   to the pull request.
