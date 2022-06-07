-- author: juhnke_r

create table person
(
    person_id  int,
    first_name varchar(40),
    last_name  varchar(40)
);

create index ix_person_1 on person(person_id);
