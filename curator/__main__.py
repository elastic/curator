
"""Executed when package directory is called as a script"""

from .curator import main
aws_flag = False # If we're using AWS Elasticsearch, this should be true
main()
