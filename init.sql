drop table if exists animaldim;
create table animaldim(
   animal_dim_key serial primary key,
   animal_id varchar,
   animal_type varchar,
   animal_name varchar,
   dob date,
   breed varchar,
   color varchar,
   sex varchar,
   timestmp timestamp
);

drop table if exists timingdim;
create table timingdim(
   time_dim_key serial primary key,
   mnth varchar,
   yr int
);

drop table if exists outcomedim;
create table outcomedim(
   outcome_dim_key serial primary key,
   outcome_type varchar,
   outcome_subtype varchar
);

drop table if exists outcomesfact;
create table outcomesfact(
   outcomesfact_key serial primary key,
   outcome_dim_key int references outcomedim(outcome_dim_key),
   animal_dim_key int references animaldim(animal_dim_key),
   time_dim_key int references timingdim(time_dim_key)
);

drop table if exists temp_table;
create table temp_table(
   temp_key serial primary key,
   animal_id varchar,
   animal_type varchar,
   animal_name varchar,
   dob date,
   breed varchar,
   color varchar,
   sex varchar,
   timestmp timestamp,
   mnth varchar,
   yr int,
   outcome_type varchar,
   outcome_subtype varchar
);
