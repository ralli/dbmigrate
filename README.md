# Database Migrations

This project implements an experimental database migration tool.

It implements two different types of migrations:

1. classical migrations, known from tools like [liquibase](https://www.liquibase.org/)
   and [flyway](https://flywaydb.org/)
2. script migrations for database objects like views and stored procedures which may be executed multiple times but only
   if the view / procedure actually changed.

## Classical migrations

Classical migrations work like expected. Migrations are written in SQL-Format and are executed in sorted order.
`202206061113_create_table_person.sql` will be executed before `202206061115_create_table_address.sql`.

An example migration:

```sql
create table person
(
    person_id  int,
    first_name varchar(40),
    last_name  varchar(40)
);

create index ix_person_1 on person(person_id);

-- optional rollback statement 
-- rollback: drop table person;
```

## Script migrations

Some database changes made do not need to be executed once only.
If views and stored procedures are changed, they may be recreated without deleting existing data - as in the case of
database tables.

* Develop like in classical software projects: Views and stored procedures are developed like classical software. Stored
  procedures and views will be developed in classical database tools.
* Only modified artifacts will be deployed: When it comes to packaging, only artifacts which have been modified will be
  deployed to the database.
* The deployment script will keep track of all script changes deployed to the database.

### Dependency graph

Dependencies in scripts are currently managed by annotation comments.

The following example shows a view with dependency on views declared in files named 
`stg__person.sql` and `stg__email.sql`. 

```sql
-- 
-- depends: stg__person, stg__email
--
create or replace view person_details as
(
    select stg__person.id,
           stg__person.first_name,
           stg__person.last_name,
           stg__email.email
    from stg__person
             join stg__email on stg__email.person_id = stg__person.person_id
)
```
