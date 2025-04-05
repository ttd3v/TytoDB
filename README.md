## TytoDB Commands

### Alba Query Language (AQL)

#### CREATE

-   **CREATE CONTAINER**: Defines a new container (table) within the database.

    ```
    CREATE CONTAINER <name> [col_nam][col_typ]
    ```

#### DELETE

-   **DELETE CONTAINER**: Removes a container from the database.

    ```
    DELETE CONTAINER <container>
    ```

### Data Manipulation Language (DML)

#### CREATE

-   **CREATE ROW**: Adds a new row of data to a specified container.

    ```
    CREATE ROW [col_nam][col_val] ON <container:name>
    ```

#### EDIT

-   **EDIT ROW**: Modifies existing data within a row in a specified container, based on given conditions.

    ```
    EDIT ROW [col_name][col_val] ON <container:name> WHERE <conditions>
    ```

#### DELETE

-   **DELETE ROW**: Removes a row from a specified container.  Can be used with or without conditions.

    ```
    DELETE ROW ON <container> WHERE <conditions>
    DELETE ROW ON <container>
    ```

### Data Query Language (DQL)

#### SEARCH

-   **SEARCH**: Retrieves data from a specified container, optionally filtered by conditions.

    ```
    SEARCH <col_nam> ON <container>
    SEARCH <col_nam> ON <container> WHERE <conditions>