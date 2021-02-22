# DPTSI Metadata Neo4j ETL

## Deskripsi

ETL untuk menyalin data dari database (mysql:8) `metadata` ke dalam bentuk graph database (neo4j).

## Konfigurasi

Salin berkas `.env.example` menjadi `.env`

```bash
cp .env.example .env # bash / powershell
```

Atur tiap nilai pada berkas `.env`

Contoh:

```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=database
MYSQL_USER=root
MYSQL_PASSWORD=password

NEO4J_HOST=localhost
NEO4J_PORT=7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
```

Lanjut ke pemasangan dependensi.

## Pemasangan Dependensi

### Menggunakan Virtual Environments

Buat dahulu virtual environmentsnya

```bash
virtualenv3 venv
# atau
python3 -m venv venv
# atau
python3 -m virtualenv venv
```

Masuk ke virtual environments

```bash
source venv/bin/activate # pada *Nix os
venv\Scripts\activate # pada Windows
```

Memasang dependensi dengan `pip`

```bash
pip install -r requirements.txt
```

Program dapat dijalakan dengan menjalankan `run.py`

```bash
python run.py
deactivate # Untuk keluar dari virtual environments
```

### Tanpa Menggunakan Virtual Enviroments

Pasang dependensi dengan `pip3`.

```bash
pip3 install -r requirements.txt
```

Program dapat dijalakan dengan menjalankan `run.py`

```bash
python3 run.py
```

## TODO

- Menambahkan Logging
- Memikirkan dan Menambahkan kasus yang belum terfasilitasi
