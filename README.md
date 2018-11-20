### CS327E Fall 2018 Snippets Repo
##### This repo contains code snippets and wiki guides.


To create the example college database used in class:
1. Download and extract [college_dataset.zip](https://github.com/cs327e-fall2018/snippets/blob/master/college_dataset.zip)
2. Load each csv file as a separate table into a BQ dataset named *college_raw*
3. Create a BQ dataset named *college_split1* and run [college_split1_ctas_stmts.sql](https://github.com/cs327e-fall2018/snippets/college_split1_ctas_stmts.sql)
4. Create a BQ dataset named *college_split2* and run the Beam scripts [format_student_dob.py](https://github.com/cs327e-fall2018/snippets/blob/master/format_student_dob.py), [normalize_takes_cno.py](https://github.com/cs327e-fall2018/snippets/blob/master/normalize_takes_cno.py), [merge_student_tables.py](https://github.com/cs327e-fall2018/snippets/blob/master/merge_student_tables.py), [dedup_student_table.py](https://github.com/cs327e-fall2018/snippets/blob/master/dedup_student_table.py), [create_student_view.py](https://github.com/cs327e-fall2018/snippets/blob/master/create_student_view.py)
