-- CTAS statements to create the initial college split tables from the raw tables. 

create table college_split1.Class as
select distinct cno, cname, credits 
from college_raw.Teacher
where cno is not null; 

create table college_split1.Student as
select distinct sid, fname, lname, dob
from college_raw.Student;

create table college_split1.New_Student as
select * from college_raw.New_Student;

create table college_split1.Takes as
select distinct sid, cno, grade
from college_raw.Student
where sid is not null and cno is not null;

create table college_split1.Teacher as
select distinct tid, instructor, dept
from college_raw.Teacher
where tid is not null; 

create table college_split1.Teaches as
select distinct tid, cno
from college_raw.Teacher
where tid is not null and cno is not null;
