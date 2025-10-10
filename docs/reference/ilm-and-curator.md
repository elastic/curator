---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/client/curator/current/ilm-and-curator.html
---

# ILM and Curator! [ilm-and-curator]

::::{warning}
Curator will not act on any index associated with an ILM policy without setting `allow_ilm_indices` to `true`.
::::


Curator and ILM *can* coexist. However, to prevent Curator from accidentally interfering, or colliding with ILM policies, any index associated with an ILM policy name is excluded by default. This is true whether you have a Basic license or not, or whether the ILM policy is enabled or not.

Curator can be configured to work with ILM-enabled indices by setting the [`allow_ilm_indices`](/reference/option_allow_ilm.md) option to `true` for any action.

Learn more about Index Lifecycle Management [here](http://www.elastic.co/guide/en/elasticsearch/reference/8.15/index-lifecycle-management.md).

