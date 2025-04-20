# TytoDB

TytoDB is a custom database system written in Rust, designed to handle structured data with support for transactions, concurrency, and networked access. It features its own query language for performing operations like creating containers (similar to tables), inserting, updating, and deleting rows, as well as searching for data with conditions.

## Features

- **Custom Query Language**: Easy-to-use syntax for database operations.
- **Data Types**: Supports Text, Int, Bigint, Float, and Bool.
- **Transactions**: Manage changes with commit and rollback.
- **Concurrency**: Uses MVCC (Multi-Version Concurrency Control) for handling concurrent access.
- **Networking**: Allows remote access via TCP connections.
- **Security**: Employs secret keys and AES-GCM encryption for data transmission.
- **Configurable**: Settings can be adjusted via `settings.yaml`.

## Installation

To install TytoDB, you need to have Rust installed on your system. Clone the repository and build the project using Cargo:

```sh
git clone https://github.com/ttd3v/TytoDB
cd TytoDB
cargo build --release
```

## Configuration

TytoDB uses a configuration file named `settings.yaml` located in the database directory (default is `~/TytoDB`). If the file doesn't exist, default settings will be used and the file will be created. Below are the configurable options:

- `max_columns`: Maximum number of columns per container (default: 50).
- `min_columns`: Minimum number of columns per container (default: 1).
- `max_str_length`: Maximum length of string values (default: 128).
- `auto_commit`: Whether to automatically commit after each operation (default: false).
- `memory_limit`: Memory limit for query processing in bytes (default: 1048576000).
- `ip`: IP address to bind the TCP listeners (default: 127.0.0.1).
- `connections_port`: Port for connection handling (default: 153971).
- `data_port`: Port for data queries (default: 893127).
- `max_connections`: Maximum number of concurrent connections (default: 10).
- `max_connection_requests_per_minute`: Rate limiting for connection requests (default: 10).
- `max_data_requests_per_minute`: Rate limiting for data requests (default: 10000000).
- `on_insecure_rejection_delay_ms`: Delay after rejecting insecure requests (default: 100).
- `safety_level`: Security level, either "strict" or "permissive" (default: strict).
- `request_handling`: Whether to handle requests synchronously or asynchronously (default: sync).
- `secret_key_count`: Number of secret keys to generate (default: 10).

## Query Language

TytoDB uses a custom query language with the following commands:

- **CREATE CONTAINER** `<name> [column_names] [column_types]`: Creates a new container with the specified name, column names, and types.
- **CREATE ROW** `[column_names] [values] ON <container_name>`: Inserts a new row into the specified container.
- **EDIT ROW** `[column_names] [values] ON <container_name> WHERE <conditions>`: Updates rows in the specified container that match the conditions.
- **DELETE ROW ON** `<container_name> WHERE <conditions>`: Deletes rows from the specified container that match the conditions.
- **DELETE ROW ON** `<container_name>`: Deletes all rows from the specified container.
- **DELETE CONTAINER** `<container_name>`: Deletes the specified container.
- **SEARCH** `<column_names> ON <container_name> WHERE <conditions>`: Searches for rows in the specified container that match the conditions and returns the specified columns.
- **COMMIT** `[container_name]`: Commits the current transaction for the specified container or all containers if none is specified.
- **ROLLBACK** `[container_name]`: Rolls back the current transaction for the specified container or all containers if none is specified.

### Examples

1. **Creating a container**:

```sql
CREATE CONTAINER users [name, age, is_active] [TEXT, INT, BOOL]
```

2. **Inserting a row**:

```sql
CREATE ROW [name, age, is_active] ["Alice", 30, true] ON users
```

3. **Searching for rows**:

```sql
SEARCH [name, age] ON users WHERE age > 25 AND is_active = true
```

4. **Updating rows**:

```sql
EDIT ROW [age] [31] ON users WHERE name = "Alice"
```

5. **Deleting rows**:

```sql
DELETE ROW ON users WHERE age < 18
```

6. **Committing changes**:

```sql
COMMIT
```

## Data Storage

TytoDB stores data in files within the database directory:

- **settings.yaml**: Configuration settings.
- **containers.yaml**: List of containers.
- **<container_name>**: Individual container data files.
- **rf/**: Directory for storing text data that exceeds the `max_str_length`.

For text fields, if the content exceeds `max_str_length`, it is stored in a separate file in the `rf` directory, and the main data file contains an ID referencing that file.

## Networking and Security

TytoDB listens on two TCP ports:

- **Connections Port**: For establishing connections (default: 153971).
- **Data Port**: For sending queries and receiving data (default: 893127).

Data transmission is encrypted using AES-GCM and compressed with LZMA. Authentication is handled via secret keys stored in `~/.TytoDB.keys`.

## Transactions and Concurrency

TytoDB supports transactions with commit and rollback operations. It uses MVCC to handle concurrent access, ensuring data consistency and isolation between transactions.

## Performance

TytoDB is optimized for handling large datasets with features like:

- **Memory Management**: Configurable memory limits for query processing.
- **Pagination**: Queries are paginated to manage memory usage efficiently.
- **Compression**: Data is compressed using LZMA for transmission.

## Troubleshooting

- **Connection Issues**: Ensure the database is running and the correct ports are open.
- **Query Errors**: Verify the syntax of your queries against the documentation.
- **Performance Issues**: Adjust the `memory_limit` in `settings.yaml` if necessary.

## Contributing

Contributions are welcome! Please fork the repository and submit pull requests with your changes.

## License

This project is licensed under the MIT License.