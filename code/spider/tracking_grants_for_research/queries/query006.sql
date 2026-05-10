SELECT T1.organisation_type FROM Organisations AS T1 JOIN Research_Staff AS T2 ON T1.organisation_id  =  T2.employer_organisation_id GROUP BY T1.organisation_type ORDER BY count(*) DESC LIMIT 1;
