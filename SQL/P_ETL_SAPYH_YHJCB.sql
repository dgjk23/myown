Create   Proc   P_ETL_SAPYH_YHJCB   AS

DELETE  FROM    ETL_SAPYH_YHJCB;
INSERT  INTO    ETL_SAPYH_YHJCB
SELECT  t2.times_type,t2.times,t2.Address,sum(t2.A�ɹ�)  �ɹ�,sum(t2.Aʧ��) ʧ��,sum(t2.A����) ���� 
FROM
	(SELECT  '����'  times_type,convert(varchar(7),createtime,121)  times,COUNT(A����) AS A���� 
	,COUNT(CASE WHEN A�ɹ� IS NULL THEN NULL ELSE A�ɹ� END ) AS A�ɹ�
	,COUNT(Aʧ��)AS Aʧ��
	,CASE WHEN geor.Addressch  IS NULL THEN 'δ֪' ELSE geor.Addressch END AS Address  
	FROM  
		(SELECT 
		MAX(CASE WHEN  logontype='A' THEN asqe ELSE NULL END) AS A����,
		MAX(CASE WHEN logonstate=1 AND logontype='A' THEN asqe ELSE NULL END) AS A�ɹ�,
		MAX(CASE WHEN logonstate=0 AND logontype='A' THEN asqe ELSE NULL END) AS Aʧ��,
		createtime,logonstate,alguser ,logontype
		--INTO  #tb_all_source 
		FROM 
			(select asqe ,CONVERT(datetime, left((CONVERT(varchar(100), createtime, 23)),7)+'-01') as createtime
			,logonstate	,alguser ,logontype 
			FROM SAPLOGIN_SYNC 
			) as a 
		WHERE logontype='A'
		GROUP BY  CreateTime,LogonState,logontype,Alguser) t1
	LEFT JOIN TB_MN_EmployeesInfo info ON t1.alguser = info.Emplid 
	LEFT JOIN TB_SimulatedGeographyNew  geor  on  info.AddressEn=geor.AddressEn
	WHERE   convert(varchar(4),createtime,121) = convert(varchar(4),getdate(),121)
			and  convert(varchar(10),createtime,121)<=convert(varchar(10),DATEADD(day,-0,getdate()),121)
	GROUP BY geor.Addressch,convert(varchar(7),createtime,121)

	UNION   ALL
	SELECT  '6����'  times_type,convert(varchar(7),createtime,121)  times,COUNT(A����) AS A���� 
	,COUNT(CASE WHEN A�ɹ� IS NULL THEN NULL ELSE A�ɹ� END ) AS A�ɹ�
	,COUNT(Aʧ��)AS Aʧ��
	,CASE WHEN geor.Addressch  IS NULL THEN 'δ֪' ELSE geor.Addressch END AS Address  
	FROM  
		(SELECT 
		MAX(CASE WHEN  logontype='A' THEN asqe ELSE NULL END) AS A����,
		MAX(CASE WHEN logonstate=1 AND logontype='A' THEN asqe ELSE NULL END) AS A�ɹ�,
		MAX(CASE WHEN logonstate=0 AND logontype='A' THEN asqe ELSE NULL END) AS Aʧ��,
		createtime,logonstate,alguser ,logontype
		--INTO  #tb_all_source 
		FROM 
			(select asqe ,CONVERT(datetime, left((CONVERT(varchar(100), createtime, 23)),7)+'-01') as createtime
			,logonstate	,alguser ,logontype 
			FROM SAPLOGIN_SYNC 
			) as a 
		WHERE logontype='A'
		GROUP BY  CreateTime,LogonState,logontype,Alguser) t1
	LEFT JOIN TB_MN_EmployeesInfo info ON t1.alguser = info.Emplid 
	LEFT JOIN TB_SimulatedGeographyNew  geor  on  info.AddressEn=geor.AddressEn
	WHERE  1=1	and  convert(varchar(10),createtime,121)>=CONVERT(varchar(10),dateadd(month,-6,getdate()),121)
	GROUP BY geor.Addressch,convert(varchar(7),createtime,121)
	UNION   ALL
	SELECT  '3����'  times_type,convert(varchar(7),createtime,121)  times,COUNT(A����) AS A���� 
	,COUNT(CASE WHEN A�ɹ� IS NULL THEN NULL ELSE A�ɹ� END ) AS A�ɹ�
	,COUNT(Aʧ��)AS Aʧ��
	,CASE WHEN geor.Addressch  IS NULL THEN 'δ֪' ELSE geor.Addressch END AS Address  
	FROM  
		(SELECT 
		MAX(CASE WHEN  logontype='A' THEN asqe ELSE NULL END) AS A����,
		MAX(CASE WHEN logonstate=1 AND logontype='A' THEN asqe ELSE NULL END) AS A�ɹ�,
		MAX(CASE WHEN logonstate=0 AND logontype='A' THEN asqe ELSE NULL END) AS Aʧ��,
		createtime,logonstate,alguser ,logontype
		--INTO  #tb_all_source 
		FROM 
			(select asqe ,CONVERT(datetime, left((CONVERT(varchar(100), createtime, 23)),7)+'-01') as createtime
			,logonstate	,alguser ,logontype 
			FROM SAPLOGIN_SYNC 
			) as a 
		WHERE logontype='A'
		GROUP BY  CreateTime,LogonState,logontype,Alguser) t1
	LEFT JOIN TB_MN_EmployeesInfo info ON t1.alguser = info.Emplid 
	LEFT JOIN TB_SimulatedGeographyNew  geor  on  info.AddressEn=geor.AddressEn
	WHERE  1=1	and  convert(varchar(10),createtime,121)>=CONVERT(varchar(10),dateadd(month,-3,getdate()),121)
	GROUP BY geor.Addressch,convert(varchar(7),createtime,121)
	UNION   ALL
	SELECT  '����'  times_type,substring(convert(varchar(10),createtime,121),9,2)  times,COUNT(A����) AS A���� 
	,COUNT(CASE WHEN A�ɹ� IS NULL THEN NULL ELSE A�ɹ� END ) AS A�ɹ�
	,COUNT(Aʧ��)AS Aʧ��
	,CASE WHEN geor.Addressch  IS NULL THEN 'δ֪' ELSE geor.Addressch END AS Address  
	FROM  
		(SELECT 
		MAX(CASE WHEN  logontype='A' THEN asqe ELSE NULL END) AS A����,
		MAX(CASE WHEN logonstate=1 AND logontype='A' THEN asqe ELSE NULL END) AS A�ɹ�,
		MAX(CASE WHEN logonstate=0 AND logontype='A' THEN asqe ELSE NULL END) AS Aʧ��,
		createtime,logonstate,alguser ,logontype
		--INTO  #tb_all_source 
		FROM 
			(select asqe ,CONVERT(datetime, left((CONVERT(varchar(100), createtime, 23)),7)+'-01') as createtime
			,logonstate	,alguser ,logontype 
			FROM SAPLOGIN_SYNC 
			) as a 
		WHERE logontype='A'
		GROUP BY  CreateTime,LogonState,logontype,Alguser) t1
	LEFT JOIN TB_MN_EmployeesInfo info ON t1.alguser = info.Emplid 
	LEFT JOIN TB_SimulatedGeographyNew  geor  on  info.AddressEn=geor.AddressEn
	WHERE  1=1	and  convert(varchar(7),createtime,121) = convert(varchar(7),DATEADD(MONTH,-1,GETDATE()),121)
	GROUP BY geor.Addressch,substring(convert(varchar(10),createtime,121),9,2)
	UNION   ALL
	SELECT  '����'  times_type,substring(convert(varchar(10),createtime,121),9,2)  times,COUNT(A����) AS A���� 
	,COUNT(CASE WHEN A�ɹ� IS NULL THEN NULL ELSE A�ɹ� END ) AS A�ɹ�
	,COUNT(Aʧ��)AS Aʧ��
	,CASE WHEN geor.Addressch  IS NULL THEN 'δ֪' ELSE geor.Addressch END AS Address  
	FROM  
		(SELECT 
		MAX(CASE WHEN  logontype='A' THEN asqe ELSE NULL END) AS A����,
		MAX(CASE WHEN logonstate=1 AND logontype='A' THEN asqe ELSE NULL END) AS A�ɹ�,
		MAX(CASE WHEN logonstate=0 AND logontype='A' THEN asqe ELSE NULL END) AS Aʧ��,
		createtime,logonstate,alguser ,logontype
		--INTO  #tb_all_source 
		FROM 
			(select asqe ,CONVERT(datetime, left((CONVERT(varchar(100), createtime, 23)),7)+'-01') as createtime
			,logonstate	,alguser ,logontype 
			FROM SAPLOGIN_SYNC 
			) as a 
		WHERE logontype='A'
		GROUP BY  CreateTime,LogonState,logontype,Alguser) t1
	LEFT JOIN TB_MN_EmployeesInfo info ON t1.alguser = info.Emplid 
	LEFT JOIN TB_SimulatedGeographyNew  geor  on  info.AddressEn=geor.AddressEn
	WHERE  1=1	and  convert(varchar(7),createtime,121)=CONVERT(varchar(7),dateadd(month,-0,getdate()),121)
	GROUP BY geor.Addressch,substring(convert(varchar(10),createtime,121),9,2))  t2
GROUP  BY   t2.times_type,t2.times,t2.Address;



/*
EXEC   P_ETL_SAPYH_YHJCB;
SELECT  *  FROM  ETL_SAPYH_YHJCB;

*/