SELECT T1.organisation_id ,  count(*) FROM Projects AS T1 JOIN Project_Outcomes AS T2 ON T1.project_id  =  T2.project_id GROUP BY T1.organisation_id ORDER BY count(*) DESC LIMIT 1;
