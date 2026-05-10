SELECT T1.project_details FROM Projects AS T1 JOIN Project_Outcomes AS T2 ON T1.project_id  =  T2.project_id WHERE T2.outcome_code  =  'Paper' INTERSECT SELECT T1.project_details FROM Projects AS T1 JOIN Project_Outcomes AS T2 ON T1.project_id  =  T2.project_id WHERE T2.outcome_code  =  'Patent';

