from neo4j import GraphDatabase


class Neo4j:
    def __init__(self, host, port, username, password):
        self.__uri = f"bolt://{host}:{port}"
        self.__driver = GraphDatabase.driver(
            self.__uri, auth=(username, password))

    def clean(self):
        with self.__driver.session() as session:
            session.run('MATCH (n) DETACH DELETE n')

    def insert_all(self, clean_data):
        with self.__driver.session() as session:
            for item in clean_data:
                session.run(item['query'], item['data'])

    def close(self):
        self.__driver.close()
