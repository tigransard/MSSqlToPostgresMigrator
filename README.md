# MS SQL Database -> Postgresql Database Migrator

[![License: Dual (NC + Commercial)](https://img.shields.io/badge/license-dual--license-blueviolet.svg)](./LICENSE)

This command line tool allows you to easily migrate databases from existing MS SQL database to PostgreSQL. 
The project is written in c#, so it is cross-platform and can run on any operating system that has a dotnet runtime version 9 or later.

While application is running, it temporary dumps all the data on the executing machine and upload it back to PostgreSQL server, so please check if you have enoght free space on your PC.

## Attention
Application is converting tables structure only and migrates data. 
It doesn't translate or migrate views, triggers, functions or any other type of data structure or procedures.

---

# Requirements

- dotnet runtime v9 or later


# Configuration

Application uses 2 json files for a specific mappings 
- type_map.json - defines a mapping between MS SQL and PostgreSQL datatypes
- computed_columns_map.json - defines a list of computed columns for PostgreSQL


# Usage

### on Windows
	DbMigrator.exe 
	  [--sql-server <host>] [--sql-port <port>] --sql-db <db> [--sql-user <user>] [--sql-pass <pass>]
      [--pg-host <host>] [--pg-port <port>] [--pg-db <db>] [--pg-user <user>] [--pg-pass <pass>]
      [--type-map <file>]
      [--columns-map <file>]
      [--base-folder <path>]
      [--verify-tables]
      [--verify-data-only]
      [--logs-to <file|console|both>]


# License

This project is released under a **dual license**:

- **Non-Commercial License (default)**  
  The source code is free to use for **personal, educational, or internal (non-commercial)** purposes.  
  See [LICENSE](./LICENSE) for details.  

- **Commercial License**  
  If you want to use this software for **client work, resale, SaaS, or any other commercial purposes**, you must obtain a commercial license.  
  See [LICENSE_COMMERCIAL](./LICENSE_COMMERCIAL) for details.

To purchase a commercial license, please contact:
**[Owner] – [owner of this repository]**
