import sys
from os import environ
from settings import setup, clean, eprint, oprint
from Extractors import Mysql
from Transformers import CleanMysql
from Loaders import Neo4j
from mysql.connector.errors import InterfaceError, ProgrammingError
from neo4j.exceptions import ServiceUnavailable, AuthError
from pprint import pprint

def extract(mysql_connection):
    results = mysql_connection.get()
    mysql_connection.close()
    return results


def transform(raw_data):
    cleaner = CleanMysql()
    cleaner.clean(raw_data)
    clean_data = cleaner.get()
    return clean_data


def loading(neo4j_connection, clean_data):
    neo4j_connection.clean()
    neo4j_connection.insert_all(clean_data)
    neo4j_connection.close()
    return True


def handleError(errors):
    for e in errors:
        eprint(e)
    oprint('Operasi dihentikan')
    sys.exit(1)


if __name__ == '__main__':
    # Import .env ke environment
    setup()

    # Setup koneksi ke mysql dan neo4j berdasarkan environment variables
    try:
        oprint('Menghubungkan ke MySQL')
        mysql = Mysql(
            host=environ['MYSQL_HOST'],
            port=environ['MYSQL_PORT'],
            username=environ['MYSQL_USER'],
            password=environ['MYSQL_PASSWORD'],
            database=environ['MYSQL_DATABASE']
        )
        oprint('MySQL Terhubung')
        oprint('Menghubungkan ke Neo4j')
        neo4j = Neo4j(
            host=environ['NEO4J_HOST'],
            port=environ['NEO4J_PORT'],
            username=environ['NEO4J_USER'],
            password=environ['NEO4J_PASSWORD']
        )
        oprint('Neo4j Terhubung')
    except KeyError:
        handleError(['Enrivorment variable (.env) gagal diakses'])
    except ProgrammingError:
        handleError(["Terjadi kesalahan:", sys.exc_info()[0], 'Pastikan kredensial mysql (.env) benar'])
    except InterfaceError:
        handleError(['Koneksi ke server mysql gagal'])
    except ServiceUnavailable:
        handleError(['Koneksi ke server neo4j gagal', 'Pastikan server neo4j berjalan'])
    except AuthError:
        handleError(['Gagal tersambung ke neo4j', 'Pastikan kredensial neo4j (.env) benar'])
    except:
        handleError(["Terjadi kesalahan:", sys.exc_info()[0]])

    oprint('Mengambil data dari MySQL')
    raw_data = extract(mysql)
    oprint('Mengolah data dari MySQL')
    clean_data = transform(raw_data)
    oprint('Menyimpan data olahan ke Neo4j')
    success = loading(neo4j, clean_data)

    oprint('Operasi Berhasil!') if success else oprint('Operasi Gagal!')
    clean()