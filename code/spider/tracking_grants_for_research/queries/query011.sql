SELECT T1.project_id ,  count(*) FROM Project_Staff AS T1 JOIN Projects AS T2 ON T1.project_id  =  T2.project_id GROUP BY T1.project_id ORDER BY count(*) ASC;
