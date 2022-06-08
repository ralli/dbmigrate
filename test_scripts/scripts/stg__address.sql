-- sources: pagila.address

create view stg__address as
(
select *
from person
order by last_name
    );
