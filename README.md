# Update DMOZ categories and domains categories

Forked from [dmo2db](https://github.com/JoKnopp/dmoz2db).

# Installation

- Make sure you have pip and sqlalchemy 0.6.5 or higher installed

- Download `structure.rdf.u8` from [DMOZ](http://rdf.dmoz.org/rdf/)

- Download `content.rdf.u8` from [DMOZ](http://rdf.dmoz.org/rdf/)

- Create database named `dmoz` `createdb dmoz`

- Copy `src/db.sample.conf` to `db.conf` and update config

```bash
# Should be run from src folder
python dmoz2db.py --keep-db -s structure.rdf.u8 -c content.rdf.u8
```

- Normalize table by renaming column and table names `psql dmoz < src/normalize.sql`

- Backup tables and upload them to live db server
  `pg_dump --table headlines_domains_categories --table headlines_categories --data-only dmoz > dmoz_categories.sql`
