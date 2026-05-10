SELECT count(*) ,  T1.project_details FROM Projects AS T1 JOIN Tasks AS T2 ON T1.project_id  =  T2.project_id GROUP BY T1.project_id;
