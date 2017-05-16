import ibis
from .bq import BQHandler
from .impala_to_sql import _impala_to_sql

BQ_IBIS_THPE_DICT = {'INTEGER': 'int64', 'BOOLEAN': 'boolean', 'STRING': 'string', 'FLOAT': 'float64', 'TIMESTAMP':'timestamp'}


class BQOrm:
    '''
    now we use ibis exp to do all the orm job
    '''

    def __init__(self, bq_handler):
        assert isinstance(bq_handler, BQHandler), "%r is not a BQHandler" % bq_handler
        self.bq_handler = bq_handler

    def build_ibis_tb(self, bq_tb_addr):
        field_list = self.bq_handler.get_tb_resource(bq_tb_addr)['schema']['fields']
        return ibis.table([(i['name'], BQ_IBIS_THPE_DICT[i['type']]) for i in field_list], bq_tb_addr)

    def get_sql(self, ibis_exp, format='sql'):
        impala_query = """#standardSQL
%s"""%(ibis.impala.compile(ibis_exp))
        if format == 'impala':
            return impala_query
        sql_query = _impala_to_sql(impala_query)
        return sql_query

    def get_df(self, ibis_exp):
        sql_query = self.get_sql(ibis_exp)
        return self.bq_handler.get_df(sql_query, legacy=False)
