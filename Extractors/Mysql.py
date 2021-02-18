from mysql import connector


class Mysql:
    def __init__(self, host, port, username, password, database):
        self.__driver = connector.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        self.__cursor = self.__driver.cursor(dictionary=True)

    def get(self):
        self.query = '''
            select 
                scsim.name as sc_sim,
                scdb.name as sc_db,
                sctbl.schema as sc_sch,
                sctbl.name as sc_tbl,
                sccol.name as sc_col,
                dtsim.name as dt_sim,
                dtdb.name as dt_db,
                dttbl.name as dt_tbl,
                dttbl.schema as dt_sch,
                dtcol.name as dt_col,
                dcmap.expression as expression,
                mt.name as pipeline_type,
                t.name as pipeline_name,
                con.name as consumer_name,
                prod.name as producer_name,
                prodtype.name as producer_type
            from data_map dm
            inner join `consumer` con on dm.consumer_id=con.consumer_id
            inner join `producer` prod on dm.producer_id=prod.producer_id
            inner join `producer_type` prodtype on prod.producer_type_id=prodtype.producer_type_id
            inner join `map_type` mt on dm.map_type_id=mt.map_type_id
            inner join `topic` t on dm.topic_id=t.topic_id
            inner join `sim` scsim on dm.sim_source_id=scsim.sim_id
            inner join `database` scdb on dm.database_source_id=scdb.database_id
            inner join `table` sctbl on dm.table_source_id=sctbl.table_id
            inner join `sim` dtsim on dm.sim_destination_id=dtsim.sim_id
            inner join `database` dtdb on dm.database_source_id=dtdb.database_id
            inner join `table` dttbl on dm.table_destination_id=dttbl.table_id
            inner join `table_type` scttype on sctbl.table_type_id=scttype.table_type_id
            inner join `table_type` dttype on dttbl.table_type_id=dttype.table_type_id
            inner join `data_column_map` dcmap on dm.data_map_id=dcmap.data_map_id
            inner join `table_column` sccol on dcmap.source_column_id=sccol.table_column_id
            inner join `table_column` dtcol on dcmap.source_column_id=dtcol.table_column_id
        '''
        self.__cursor.execute(self.query)
        return self.__cursor.fetchall()

    def close(self):
        self.__driver.close()
