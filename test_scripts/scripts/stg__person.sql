-- sources: pagila.person

create view stg__person as
(
select *
from pagila.person
order by last_name
    );
