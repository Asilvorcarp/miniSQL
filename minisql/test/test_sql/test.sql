-- execfile "test.sql";

-- 1. 创建数据库db0、db1、db2，并列出所有的数据库
create database db0;
create database db1;
create database db2;
show databases;
use db0;




-------- my test START --------

create table t1(a int, b char(20) unique, c float, primary key(a, c));
create table student(sno char(8), sage int, sab float unique, primary key (sno, sab));
create table pets(name char(8), id int, price float, primary key (name, id));
-- create table pets(name char(8), id int, price float unique, primary key (name, id));
show tables;
insert into pets values("cat", 1, 11);
insert into pets values("cat", 2, 22);
insert into pets values("cat", 3, 33);
insert into pets values("cat", 4, 44);
insert into pets values("dog", 1, 55);
insert into pets values("dog", 2, 66);
insert into pets values("dog", 3, 77);
insert into pets values("dog", 4, 88);

create index priceIndex on pets(price);

create index petsId on pets(id);
-- error: key is not unique
create index nameId on pets(name, id);

-- show index from pets; -- not supported
show indexes;

insert into pets values("pig", 1, 88);
-- error: UNIQUE约束冲突(price)
insert into pets values("cat", 1, 99);
-- error: PRIMARY KEY约束冲突(name, id)

select * from pets where name = "cat";
select * from pets where name = "cat" and id = 3;

-------- my test END --------



-- 2.在db0数据库上创建数据表account，表的定义如下
-- name not unique for "在实现中自动为UNIQUE列建立B+树索引"
create table account( id int, name char(16), balance float, primary key(id));

-- 3.考察SQL执行以及数据插入操作：
execfile "account_input.sql";
select * from account;

-- 4. 考察点查询操作：
select * from account where id = 12509999;
select * from account where balance = 86.27;
select * from account where name = "name56789";
-- 此处记录执行时间 t1
select * from account where id <> 12509999;
select * from account where balance <> 86.27;
select * from account where name <> "name56789";

-- 5.考察多条件查询与投影操作：
select id, name from account where balance >= 100 and balance < 200;
select id, name, balance from account where balance > 750 and id <= 12500500;

-- 6.考察唯一约束：
insert into account values(12556789, "name56789", 999.99);
-- 提示PRIMARY KEY约束冲突或UNIQUE约束冲突 // !! Need Add SameBalance !! 
insert into account values(111, "SameBalance1", 543.21); 
insert into account values(222, "SameBalance2", 543.21);
create index idx01 on account(balance);
-- 提示只能在唯一键上建立索引 // !! Need Add SameBalance !! 

-- 7. 考察索引的创建删除操作、记录的删除操作以及索引的效果：
select * from account where name = "name56789";
-- 此处记录执行时间 t1
create index idx02 on account(name);

-- new:
insert into account values(123123, "name56789", 555.55);
-- 提示 UNIQUE约束冲突 (name)

select * from account where name = "name56789";
-- t2 < t1
select * from account where name = "name45678";
-- t3
delete from account where name = "name45678";
insert into account values(12545678, "name45678", 233.45);

------ test update START ------
    select * from account where name = "name45678";
    update account set balance = 300 where name = "name45678";
    select * from account where name = "name45678";
------ test update END ------

drop index idx01;
select * from account where name = "name56789";
-- 执行(c)的语句 t4 > t3
select * from account where name = "name45678";
-- 执行(b)的语句 t5 > t2

-- 8. 考察删除操作：
select * from account where balance = 300;
delete from account where balance = 300;
-- 并通过select操作验证记录被删除
select * from account where balance = 300;

select * from account where name = "name56789";
delete from account;
-- 并通过select操作验证全表被删除
select * from account;

select * from account where name = "name56789";
drop table account;
-- 并通过show tables验证该表被删除
select * from account;