create database if not exists azhive;
use azhive;
create table if not exists aztest(id string,name string) row format delimited fields terminated by '\t';
