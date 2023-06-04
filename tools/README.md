# Migration Tool

Use the `migrate.py` script to import PTB generated pickle persistence files into a PostgreSQL database.

## Dependencies

- `psycopg[binary]` >= 3.0.13

## Usage

Clone the repository and copy the `config.json.sample` file to `config.json`. Update the configuration to match your requirements.

```
$ python migrate.py
```

The following parameters need to be defined in the `config.json` file:

- `type`: Specifies the database type. Currently supported values are `postgres`.
- `host`: The hostname of the database server.
- `port`: The port of the database server.
- `database`: The name of the database.
- `username`: The username to use when connecting to the database.
- `password`: The password to use when connecting to the database.
- `schema`: The name of the schema to use.
- `skip_null` : An optional boolean flag to skip null dictionary values.