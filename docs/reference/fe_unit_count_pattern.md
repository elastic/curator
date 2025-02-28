---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/fe_unit_count_pattern.html
---

# unit_count_pattern [fe_unit_count_pattern]

::::{note}
This setting is only used with the age filtertype to define, whether the [unit_count](/reference/fe_unit_count.md) value is taken from the configuration or read from the index name via a regular expression.
::::


```yaml
 - filtertype: age
   source: creation_date
   direction: older
   unit: days
   unit_count: 3
   unit_count_pattern: -([0-9]+)-
```

This setting can be used in cases where the value against which index age should be assessed is not a static value but can be different for every index. For this case, there is the option of extracting the index specific value from the index names via a regular expression defined in this parameter.

Consider for example the following index name patterns that contain the retention time in their name: *logstash-30-yyyy.mm.dd*, *logstash-12-yyyy.mm*, *_3_logstash-yyyy.mm.dd*.

To extract a value from the index names, this setting will be compiled as a regular expression and matched against index names, for a successful match, the value of the first capture group from the regular expression is used as the value for [unit_count](/reference/fe_unit_count.md).

If there is any error during compiling or matching the expression, or the expression does not contain a capture group, the value configured in [unit_count](/reference/fe_unit_count.md) is used as a fallback value, unless it is set to *-1*, in which case the index will be skipped.

::::{tip}
Regular expressions and match groups are not explained here as they are a fairly large and complex topic, but there are numerous resources online that will help. Using an online tool for testing regular expressions like [regex101.com](https://regex101.com/) will help a lot when developing patterns.
::::


**Examples**

* *logstash-30-yyyy.mm.dd*: Daily index that should be deleted after 30 days, indices that don’t match the pattern will be deleted after 365 days

```
 - filtertype: age
   source: creation_date
   direction: older
   unit: days
   unit_count: 365
   unit_count_pattern: -([0-9]+)-
```

* *logstash-12-yyyy.mm*: Monthly index that should be deleted after 12 months, indices that don’t match the pattern will be deleted after 3 months

```yaml
 - filtertype: age
   source: creation_date
   direction: older
   unit: months
   unit_count: 3
   unit_count_pattern: -([0-9]+)-
```

* *_3_logstash-yyyy.mm.dd*: Daily index that should be deleted after 3 years, indices that don’t match the pattern will be ignored

```yaml
 - filtertype: age
   source: creation_date
   direction: older
   unit: years
   unit_count: -1
   unit_count_pattern: ^_([0-9]+)_
```

::::{important}
Be sure to pay attention to the interaction of this parameter and [unit_count](/reference/fe_unit_count.md)!
::::


