-- depends: stg__person, stg__address

create or replace view dim_person as
(
with person as (select * from stg__person),
     addrees as (select * from stg__address),
     final_result as (select *
                      from person
                               join address on address.person_id = person.person_id)
select *
from final_result
    );
