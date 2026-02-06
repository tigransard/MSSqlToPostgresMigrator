# MS SQL Database -> Postgresql Database Migrator

[![License: Dual (NC + Commercial)](https://img.shields.io/badge/license-dual--license-blueviolet.svg)](./LICENSE)

This command line tool allows you to easily migrate databases from existing MS SQL database to PostgreSQL. 
The project is written in c#, so it is cross-platform and can run on any operating system that has a dotnet runtime version 10 or later.

While application is running, it temporary dumps all the data on the executing machine and upload it back to PostgreSQL server, so please check if you have enoght free space on your PC.

## Attention
Application is converting tables structure only and migrates data. 
It doesn't translate or migrate views, triggers, functions or any other type of data structure or procedures.

---

# Requirements

- dotnet runtime v10 or later


# Configuration

Application uses 3 json files for a specific mappings 
- [type_map.json](src/type_map.json) - defines a mapping between MS SQL and PostgreSQL datatypes. 
    1. There are already some mappings in the file
    1. If your database uses any other types or you're not agree wih the default types - change or add necessary mappings.

- [computed_columns_map.json](src/computed_columns_map.json) - defines a list of computed columns for PostgreSQL.
    1. There is only a dummy mapping in the file. 
    1. Migrator automatically will try to translate the computed columns, but sometimes it's easier to add them manually.

- [queries.json](src/queries.json) - defines a list of data validation queries
    1. to validate that the migration went well application runs queries from this file in both databases - results should match.
    1. the default queries for row counts are already included in the base functionality.
    1. add as many queries as you need to verify your important data.

# Usage

### on Windows
	DbMigrator.exe 
	  [--sql-server <host>] [--sql-port <port>] --sql-db <db> [--sql-user <user>] [--sql-pass <pass>]
      [--pg-host <host>] [--pg-port <port>] [--pg-db <db>] [--pg-user <user>] [--pg-pass <pass>]
      [--type-map <file>]
      [--columns-map <file>]
      [--queries <file>]
      [--base-folder <path>]
      [--verify-tables]
      [--verify-data-only]
      [--logs-to <file|console|both>]

### on Linux
    remove ".exe" from command line


# License

This project is released under a **dual license**:

- **Non-Commercial License (default)**  
  The source code is free to use for **personal, educational, or internal (non-commercial)** purposes.  
  See [LICENSE](./LICENSE) for details.  

- **Commercial License**  
  If you want to use this software for **client work, resale, SaaS, or any other commercial purposes**, you must obtain a commercial license.  
  See [LICENSE_COMMERCIAL](./LICENSE_COMMERCIAL) for details.

To purchase a commercial license, please contact:
**[Owner] – Tigran Sardaryan (github.com/tigransard) **
