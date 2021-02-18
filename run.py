import sys
from os import environ
from settings import setup
from Extractors import Mysql
from Transformers import CleanMysql
from Loaders import Neo4j


def extract(mysql_connection):
    # ambil seluruh data dibutuhkan, return raw data hasil kueri
    return mysql_connection.get()


def transform(raw_data):
    # terima data kasar, transform pake class cleaner/parser yang bakal dibuat
    # jangan lupa bisa tidak ada skema
    # return berupa list query neo4j dan datanya
    cleaner = CleanMysql()
    cleaner.clean(raw_data)
    return cleaner.get()


def loading(neo4j_connection, clean_data):
    # ambil daftar kueri dan data
    # jalankan di neo4j
    neo4j_connection.clean()
    neo4j_connection.insert_all(clean_data)
    neo4j_connection.close()
    return 1


    # Belum kepikiran exception
if __name__ == '__main__':
    setup()

    try:
        mysql = Mysql(
            host=environ.get('MYSQL_HOST'),
            port=environ.get('MYSQL_PORT'),
            username=environ.get('MYSQL_USER'),
            password=environ.get('MYSQL_PASSWORD'),
            database=environ.get('MYSQL_DATABASE')
        )
    except:
        print('Koneksi mysql gagal')
        print("Unexpected error:", sys.exc_info())
        sys.exit(1)

    try:
        neo4j = Neo4j(
            host=environ.get('NEO4J_HOST'),
            port=environ.get('NEO4J_PORT'),
            username=environ.get('NEO4J_USER'),
            password=environ.get('NEO4J_PASSWORD')
        )
    except:
        print('Koneksi neo4j gagal')
        print("Unexpected error:", sys.exc_info()[0])
        # sys.exit(1)

    raw_data = extract(mysql)
    clean_data = transform(raw_data)
    loading(neo4j, clean_data)
