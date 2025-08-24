# TytoDB
A columnar database, focused on being both fast and ACID-compliant.

## Getting started
To get started, you must download the database using the command below
```bash
curl -fsSL https://tytodb.pages.dev/installer.sh | bash
```
To use it, you must write commands, queries, or transactions by using connection handlers like the [TytoDB-Rust-Client](<https://github.com/FeatheredSystems/TytoDB-Rust-Client>). To configure the database or manage the program files manually, the files are usually in `~/TytoDB`, since it uses `$HOME/TytoDB` to store its files.

### Compatibility
The database is written in Rust and C, which means that it is portable across processors and systems. However, the program relies on `io_uring`, which means that the kernel must be version 5.1 or newer. If you are on an ARM device or want to compile the program yourself, the C part of the code must have its dependencies available during compilation. I suggest using static links during compilations since it makes the binary more portable across machines.  

## Features
- ACID: The database is ACID compliant, ensuring that transaction data is on disk after every commit.
- Hashmap Indexing: Indexed using an in-disk hashmap, which means that queries with equality involving the primary key will be excellent. However, it tends to be slower on writes if the device is in an HDD, which is not great at scattered operations, regardless of the code optimisations.
- API-based: The database doesn't have a query language; to communicate with the system, you must use its API.

## Contributions
The project is not accepting contributors for the codebase itself due to curatorial reasons. If you want to help the project improve, provide feedback, perform tests, and benchmarks of your own.

## Documentation
The documentation can be found in the following URL [https://tytodb.pages.dev/docs](<https://tytodb.pages.dev/docs>)

## Contact
If you find a bug, security vulnerability, have feedback, or a suggestion,
You can reach me by sending an email to the address below:

Email: `tytodatabase@gmail.com`
In case you prefer not to reach out by email and are not trying to submit a security vulnerability report, feel free to create an issue.
