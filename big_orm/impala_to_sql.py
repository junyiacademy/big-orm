'''
create an really ugly function to string wise
transform the impala sql language to SQL 2011 standard.

The SQL 2011 documents is in https://cloud.google.com/bigquery/docs/reference/standard-sql/
'''


def _impala_to_sql(ibis_exp):

    # change the bigint data type to int64 type
    ibis_exp = ibis_exp.replace("bigint", "int64")

    # change the fnv_hash function into sha1 hash function
    ibis_exp = ibis_exp.replace("fnv_hash", "sha1")

    return ibis_exp
