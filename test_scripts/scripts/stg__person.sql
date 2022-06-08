-- sources: pagila.person

create view stg__person as
(
select *
from person
order by last_name
    );
