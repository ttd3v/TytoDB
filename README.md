# TytoDB
Tyto is a database made for ensure the highest performance in cases that reads must be extremely fast, ensuring fast queries (specially for cases where the id of the row is given) and fast writes.

# Commands
For ensuring a fast processing of each execution, Tyto DB have its own language. Its commands will be documented below.
## Query commands
### PICK
Gather all the data in a given row, takes the row id and the container to be queried as arguments
```
PICK <id:int> ON <container:string>
```
### SEARCH
Gather column data(columns must be specified) on a container that matches the given conditions
```
SEARCH <columns:string[]> ON <container:string> IF <conditions>
```
example 
```
SEARCH id name age ON people IF age > 20
```
## Write commands
### INSERT
insert row data into a specified container, if the insert doesnt contain all the columns specified, an error will be thrown
```
INSERT <column(value)> ON <container:string>
```
example:
```
INSERT name('John doe') age(100) ON people
```
### DELETE
delete row data from a specified container using one or more ids

```
DELETE <id:int[]> ON <container:string>
```
example:
```
DELETE 1 2 3 4 5 10 99 111 1 84 ON people
DELETE 55 ON people
```
### DESTROY
delete all rows from a container that met the given conditions
```
DESTROY ON <container:string> IF <conditions>
```
example:
```
DESTROY ON people IF age > 100
```
### UPDATE
update row data from a specified container using one or more ids
```
UPDATE <columns(values)> ON <container:string>
```
example:
```
UPDATE name('Mr.John doe') age(67) ON people
```
### CONTAINER
creates a new container. You must specify each column type and its name
OBS: DO NOT USE THE NAME 'id' IT IS A DEFAULT COLUMN FOR EVERY CONTAINER SINCE IT IS PART OF THE DB'S SEARCHING MECHANISM
```
CONTAINER <<type>(<string>)> NAME(<string>)
```
example:
```
CONTAINER int('age') string('name') NAME('people')
```