--create a BQ dataset "hr" before running this script
 
create table hr.employee(empid int64, emp_name string, emp_dep int64);
create table hr.department(depid int64, dep_name string);

insert into hr.employee(empid, emp_name, emp_dep) values(2, 'Mike', 1); 
insert into hr.employee(empid, emp_name, emp_dep) values(23, 'Dave', 2); 
insert into hr.employee(empid, emp_name, emp_dep) values(3, 'Sarah', null);
insert into hr.employee(empid, emp_name, emp_dep) values(5, 'Jim', 4); 
insert into hr.employee(empid, emp_name, emp_dep) values(6, 'Sunil', 1);
insert into hr.employee(empid, emp_name, emp_dep) values(37, 'Morgan', 4);

insert into hr.department(depid, dep_name) values(1, 'Sales'); 
insert into hr.department(depid, dep_name) values(2, 'Product'); 
insert into hr.department(depid, dep_name) values(3, 'Research'); 
insert into hr.department(depid, dep_name) values(4, 'Engineering'); 
insert into hr.department(depid, dep_name) values(5, 'HR'); 