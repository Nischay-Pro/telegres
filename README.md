# Telegres

## Introduction

This library provides a way to add more persistence classes for [Telegram Python Bot](https://github.com/python-telegram-bot/python-telegram-bot).

Currently, the library provides support to add PostgreSQL database as a persistence class for [Telegram Python Bot](https://github.com/python-telegram-bot/python-telegram-bot).

## Installing

Currently, the library can be installed from only source code. In the future, the library will be distributed as a package.

```bash
$ git clone https://github.com/Nischay-Pro/telegres.git
$ cd telegres
$ python3 setup.py install
```

## Dependencies

`telegres` depends on the following libraries:

- `psycopg[binary]` >= 3.0.13
- `python-telegram-bot` >= 20.0a0
- `tenacity` >= 8.0.1
- `pip` >= 20.3

## Optional Dependencies

`telegres` can also be installed with the following optional dependencies:

- `ujson` >= 4.0.0. This library speeds up the performance of the [json](https://docs.python.org/3/library/json.html) standard library.

## Notes

The library is currently in alpha stage. Expect alot of bugs and performance regressions. Please report any issues to the [issue tracker](https://github.com/Nischay-Pro/telegres/issues)

## Usage

Currently, the library only supports PostgreSQL database.

Add the following import code to your bot's file:

```python
from telegres import PostgresPersistence
```

Then define a persistence object:

```python
persistence = PostgresPersistence(postgres_database="test", postgres_username="admin", postgres_password="test", postgres_host="localhost", postgres_port="5432",postgres_schema="telegres")

persistence = PostgresPersistence(postgres_url="postgres://admin:test@localhost:5432/test", postgres_schema="telegres", postgres_timestamp=True)
```

You can then include the persistence object by adding it to the ApplicationBuilder class and calling the `persistence` method:

```python
application = Application.builder().token("<your token here>").persistence(persistence).build()
```

You can either use the URL or the connection parameters.

- `postgres_database`: The name of the database. Defaults to `telegres`.
- `postgres_username`: The username of the database. Defaults to `telegres`.
- `postgres_password`: The password of the database. Defaults to `password123`.
- `postgres_host`: The host of the database. Defaults to `localhost`.
- `postgres_port`: The port of the database. Defaults to `5432`.
- `postgres_url`: The URL of the database.
- `postgres_schema`: The schema of the database. Defaults to `telegres`.
- `postgres_timestamp`: Whether to use timestamp or not. Defaults to `True`.

`postgres_schema` is useful in case you want to store multiple bot data in the same database.

## Migration

We have provided a migration tool for the library. You can use it to migrate your data from pickle persistence to PostgreSQL persistence.

```bash
git clone https://github.com/Nischay-Pro/telegres.git
cd telegres/tools
python3 migrate.py -c <config file> -p <pickle file>
```

A sample config file is provided in the `tools` directory. Update the database parameters accordingly. The pickle file is the file that you have created using the pickle persistence. Run the tool and it will automatically create the necessary tables and populate it with the data.

### Note
The `skip_null` parameter will instruct the tool to ignore empty fields. This is useful when you have a lot of empty fields in your data.