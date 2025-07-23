# TytoDB
A columnar database, focused on being both fast and ACID compliant.

## Getting started
To get started you must download the database using the command below
```bash
curl -fsSL https://tytodb.pages.dev/installer.sh | bash
```
To use it, you must write commands, queries, or transactions by using connection handlers like the [TytoDB-Rust-Client](<https://github.com/FeatheredSystems/TytoDB-Rust-Client>). To configure the database or manage the program files manually, the files are usually in `~/TytoDB`, since it uses `$HOME/TytoDB` to store its files.

### Compatibility
The database is written in Rust and C, which means that it is portable across processors and systems. However, the program relies on `io_uring`, which means that the kernel must be version 5.1 or newer. If you are on an ARM device or want to compile the program yourself, the C part of the code (`io.c`) uses a global include `#include <liburing.h>`, which must be available during compilation.  

## Features
- ACID: The database is ACID compliant, ensuring that transaction data is on disk after every commit.
- Hashmap Indexing: Indexed using an in-disk hashmap, which means that queries with equality involving the primary key will be excellent. However, it is weak on writes if the device has an HDD, which is not great at scattered operations.
- API-based: The database doesn't have a query language; to communicate with the system, you must use its API.

## Contributions
The project is not accepting contributors for the codebase itself, but people benchmarking and testing the project will help it.

## Documentation
The documentation can be found in the following URL [https://tytodb.pages.dev/docs](<https://tytodb.pages.dev/docs>)

## Contact
If you find a bug, security vulnerability, have feedback, or a suggestion,
you can reach me by sending an email to the address below:

Email: `tytodatabase@gmail.com`

## Community
If you want to be part of the project's community, join the [Discord server](<https://discord.com/invite/pjsG8YrpM7>).
