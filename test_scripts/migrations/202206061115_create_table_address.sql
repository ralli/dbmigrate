-- author: juhnke_r

create table address
(
    address_id  int,
    person_id   int,
    address1    varchar(40),
    address2    varchar(40),
    city        varchar(40),
    postal_code varchar(10)
);

create index ix_address_1 on address (address_id);
