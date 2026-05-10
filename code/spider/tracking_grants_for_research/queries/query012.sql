SELECT T1.project_details ,  T1.project_id FROM Projects AS T1 JOIN Project_Outcomes AS T2 ON T1.project_id  =  T2.project_id GROUP BY T1.project_id ORDER BY count(*) DESC LIMIT 1;
