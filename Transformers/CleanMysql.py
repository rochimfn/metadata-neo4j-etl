class CleanMysql:
    def __init__(self):
        # asumsi : - database selalu dan harus memiliki relasi ke sim
        #          - pipeline type bergantung dengan pipeline name
        self.__srcmap_wschema = '''
            MERGE (sc_sim:SIM {name: $sc_sim})
            MERGE (sc_db:Database {name: $sc_db})-[:DIMILIKI_OLEH]->(sc_sim)
            MERGE (sc_sch:Skema {name: $sc_sch})-[:DIMILIKI_OLEH]->(sc_db)
            MERGE (sc_tbl:Tabel {name: $sc_tbl})-[:DIMILIKI_OLEH]->(sc_sch)
            '''
        self.__srcmap_woschema = '''
            MERGE (sc_sim:SIM {name: $sc_sim})
            MERGE (sc_db:Database {name: $sc_db})-[:DIMILIKI_OLEH]->(sc_sim)
            MERGE (sc_tbl:Tabel {name: $sc_tbl})-[:DIMILIKI_OLEH]->(sc_db)
            '''
        self.__srcmap_wcol = '''
            MERGE (sc_col:Kolom {name: $sc_col})-[:DIMILIKI_OLEH]->(sc_tbl)
        '''
        self.__dstmap_wschema = '''
            MERGE (dt_sim:SIM {name: $dt_sim})
            MERGE (dt_db:Database {name: $dt_db})-[:DIMILIKI_OLEH]->(dt_sim)
            MERGE (dt_sch:Skema {name: $dt_sch})-[:DIMILIKI_OLEH]->(dt_db)
            MERGE (dt_tbl:Tabel {name: $dt_tbl})-[:DIMILIKI_OLEH]->(dt_sch)
            '''
        self.__dstmap_woschema = '''
            MERGE (dt_sim:SIM {name: $dt_sim})
            MERGE (dt_db:Database {name: $dt_db})-[:DIMILIKI_OLEH]->(dt_sim)
            MERGE (dt_tbl:Tabel {name: $dt_tbl})-[:DIMILIKI_OLEH]->(dt_db)
            '''
        self.__dstmap_wcol = '''
            MERGE (dt_col:Kolom {name: $dt_col})-[:DIMILIKI_OLEH]->(dt_tbl)
        '''
        self.__primary_rel = '''
            MERGE (consumer:Consumer {name: $consumer_name})
            MERGE (producer:Producer {name: $producer_name, type: $producer_type})
        '''
        self.__pipeline_rel='''
            MERGE (pipeline:Pipeline {name: $pipeline_name, type: $pipeline_type})
            MERGE (consumer)<-[:DIKONSUMSI_OLEH]-(pipeline)-[:DIPRODUKSI_OLEH]->(producer)
        '''

    def __primary_rel_func(self, source, destination):
        return '''
            MERGE (%s)-[:DIGUNAKAN_OLEH]->(producer)
            MERGE (%s)-[:DIGUNAKAN_OLEH]->(consumer)
        ''' % (source, destination)
    
    def __pipeline_rel_func(self, source, destination):
        return 'MERGE (%s)-[:SUMBER_DARI]->(pipeline)-[:TUJUAN_DARI {expression: $expression}]->(%s)' % (source, destination)

    def clean(self, raw_data):
        # Bukan pendekatan terbaik
        # Terlalu banyak meload string ke memory
        # Potensi terjadi overload memory jika data sangat banyak(?)
        self.__clean_data = []
        for data in raw_data:
            query = ''
            # Pemetaaan sumber dengan atau tanpa skema
            query += self.__srcmap_woschema if (
                data['sc_sch'] is None) else self.__srcmap_wschema

            # Pemetaan jika memiliki kolom sumber
            if (data['sc_col'] is not None):
                query+= self.__srcmap_wcol

            # Pemetaan tujuan dengan atau tanpa skema
            query += self.__dstmap_woschema if (
                data['dt_sch'] is None) else self.__dstmap_wschema

            # Pemetaan jika memiliki kolom tujuan
            if(data['dt_col'] is not None):
                query+= self.__dstmap_wcol

            # Pemetaan utama
            query += self.__primary_rel
            query += self.__primary_rel_func('sc_tbl', 'dt_tbl')
            if (data['sc_col'] is not None) or (data['dt_col'] is not None):
                source = 'sc_col' if data['sc_col'] is not None else 'sc_tbl'
                destination = 'dt_col' if data['dt_col'] is not None else 'dt_tbl'
                query += self.__primary_rel_func(source, destination)

            # Pemetaan pipeline jika valuenya ada
            if (data['pipeline_name'] is not None) and (data['pipeline_type'] is not None):
                query += self.__pipeline_rel
                query += self.__pipeline_rel_func('sc_tbl', 'dt_tbl')
                if (data['sc_col'] is not None) or (data['dt_col'] is not None):
                    source = 'sc_col' if data['sc_col'] is not None else 'sc_tbl'
                    destination = 'dt_col' if data['dt_col'] is not None else 'dt_tbl'
                    query += self.__pipeline_rel_func(source, destination)

            self.__clean_data.append({'query': query, 'data': {
                'sc_sim': data['sc_sim'],
                'sc_db': data['sc_db'],
                'sc_sch': data['sc_sch'],
                'sc_tbl': data['sc_tbl'],
                'sc_col': data['sc_col'],
                'dt_sim': data['dt_sim'],
                'dt_db': data['dt_db'],
                'dt_sch': data['dt_sch'],
                'dt_tbl': data['dt_tbl'],
                'dt_col': data['dt_col'],
                'consumer_name': data['consumer_name'],
                'producer_name': data['producer_name'],
                'producer_type': data['producer_type'],
                'pipeline_name': data['pipeline_name'],
                'pipeline_type': data['pipeline_type'],
                'expression': data['expression'] or '-',
            }})

    def get(self):
        return self.__clean_data
