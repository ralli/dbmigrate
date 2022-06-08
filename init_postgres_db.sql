-- drop database dbmigrate;

create database dbmigrate;
create role dbmigrate with login encrypted password 'dbmigrate';
alter database dbmigrate owner to dbmigrate;
grant all privileges on database dbmigrate to dbmigrate;


