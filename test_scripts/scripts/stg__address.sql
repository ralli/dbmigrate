-- sources: pagila.address

create view stg_address as
(
select *
from person
order by last_name
    );
