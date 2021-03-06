USE [itsmdata]
GO
/****** Object:  StoredProcedure [dbo].[P_ETL_SAPDD_DDJCB]    Script Date: 2016/12/21 星期三 下午 8:13:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   Proc  [dbo].[P_ETL_SAPDD_DDJCB]   AS
delete  from    ETL_SAPDD_DDJCB;
insert  into    ETL_SAPDD_DDJCB   
SELECT  t2.times_type,t2.months,t2.BUKRS, 
			sum(t2.TOTAL) TOTAL,sum(t2.NWSP) NWSP,sum(t2.NZWS) NZWS,
			sum(t2.CTOTAL) CTOTAL,sum(t2.CNWSP) CNWSP,sum(t2.CNZWS) CNZWS
FROM	
	(SELECT  '本年'  times_type,t1.months,t1.BUKRS, 
			sum(t1.TOTAL) TOTAL,sum(t1.NWSP) NWSP,sum(t1.NZWS) NZWS,
			sum(t1.CTOTAL) CTOTAL,sum(t1.CNWSP) CNWSP,sum(t1.CNZWS) CNZWS

	FROM
		(select 
			convert(varchar(4),SPMON,121)   years,
			convert(varchar(7),SPMON,121)   months,
			datepart(week,SPMON)            weeks,
			convert(varchar(10),SPMON,121)  days,
			convert(varchar(13),SPMON,121)  times,
			BUKRS,
			SUM(KZWI2) AS TOTAL,
			SUM(CASE WHEN AUART='NWSP' THEN KZWI2 ELSE 0 END ) AS NWSP,
			SUM(CASE WHEN AUART='WSP' THEN KZWI2 ELSE 0 END ) AS NZWS,
			SUM(ORQTY) AS CTOTAL,
			SUM(CASE WHEN AUART='NWSP' THEN ORQTY ELSE 0 END ) AS CNWSP,
			SUM(CASE WHEN AUART='WSP' THEN ORQTY ELSE 0 END ) AS CNZWS
		 from 
		(
				SELECT 
   
						CONVERT(DATETIME,SPMON+'01',23) as SPMON
						,DEMO1 AS  BUKRS
						,AUART
						,SUM(CAST (ORQTY AS INT)) as ORQTY
						,SUM(CAST ( KZWI2 AS float)) as KZWI2
				FROM SAPORDER_SYNC 
				WHERE TYPE=2
				GROUP BY SPMON,DEMO1,AUART
		) as aac
		WHERE  1=1  
		   and  convert(varchar(4),SPMON,121) = convert(varchar(4),getdate(),121)
		   and  convert(varchar(10),SPMON,121)<=convert(varchar(10),DATEADD(day,-0,getdate()),121)
		GROUP BY SPMON,BUKRS)  t1
	GROUP  BY  t1.BUKRS,t1.months
	union   all
	SELECT  '3个月'  times_type,t1.months,t1.BUKRS, 
			sum(t1.TOTAL) TOTAL,sum(t1.NWSP) NWSP,sum(t1.NZWS) NZWS,
			sum(t1.CTOTAL) CTOTAL,sum(t1.CNWSP) CNWSP,sum(t1.CNZWS) CNZWS
	FROM
		(select 
			convert(varchar(4),SPMON,121)   years,
			convert(varchar(7),SPMON,121)   months,
			datepart(week,SPMON)            weeks,
			convert(varchar(10),SPMON,121)  days,
			convert(varchar(13),SPMON,121)  times,
			BUKRS,
			SUM(KZWI2) AS TOTAL,
			SUM(CASE WHEN AUART='NWSP' THEN KZWI2 ELSE 0 END ) AS NWSP,
			SUM(CASE WHEN AUART='WSP' THEN KZWI2 ELSE 0 END ) AS NZWS,
			SUM(ORQTY) AS CTOTAL,
			SUM(CASE WHEN AUART='NWSP' THEN ORQTY ELSE 0 END ) AS CNWSP,
			SUM(CASE WHEN AUART='WSP' THEN ORQTY ELSE 0 END ) AS CNZWS
		 from 
		(
				SELECT 
   
						CONVERT(DATETIME,SPMON+'01',23) as SPMON
						,DEMO1 AS  BUKRS
						,AUART
						,SUM(CAST (ORQTY AS INT)) as ORQTY
						,SUM(CAST ( KZWI2 AS float)) as KZWI2
				FROM SAPORDER_SYNC 
				WHERE TYPE=2
				GROUP BY SPMON,DEMO1,AUART
		) as aac
		WHERE  1=1  
		   --and  convert(varchar(4),SPMON,121) = convert(varchar(4),getdate(),121)
		   and  convert(varchar(10),SPMON,121)>=CONVERT(varchar(10),dateadd(month,-3,getdate()),121)
		GROUP BY SPMON,BUKRS)  t1
	GROUP  BY  t1.BUKRS,t1.months
	union   all
	SELECT  '上月'  times_type,t1.months,t1.BUKRS, 
			sum(t1.TOTAL) TOTAL,sum(t1.NWSP) NWSP,sum(t1.NZWS) NZWS,
			sum(t1.CTOTAL) CTOTAL,sum(t1.CNWSP) CNWSP,sum(t1.CNZWS) CNZWS
	FROM
		(select 
			convert(varchar(4),SPMON,121)   years,
			convert(varchar(7),SPMON,121)   months,
			datepart(week,SPMON)            weeks,
			convert(varchar(10),SPMON,121)  days,
			convert(varchar(13),SPMON,121)  times,
			BUKRS,
			SUM(KZWI2) AS TOTAL,
			SUM(CASE WHEN AUART='NWSP' THEN KZWI2 ELSE 0 END ) AS NWSP,
			SUM(CASE WHEN AUART='WSP' THEN KZWI2 ELSE 0 END ) AS NZWS,
			SUM(ORQTY) AS CTOTAL,
			SUM(CASE WHEN AUART='NWSP' THEN ORQTY ELSE 0 END ) AS CNWSP,
			SUM(CASE WHEN AUART='WSP' THEN ORQTY ELSE 0 END ) AS CNZWS
		 from 
		(
				SELECT 
   
						CONVERT(DATETIME,SPMON+'01',23) as SPMON
						,DEMO1 AS  BUKRS
						,AUART
						,SUM(CAST (ORQTY AS INT)) as ORQTY
						,SUM(CAST ( KZWI2 AS float)) as KZWI2
				FROM SAPORDER_SYNC 
				WHERE TYPE=2
				GROUP BY SPMON,DEMO1,AUART
		) as aac
		WHERE  1=1  
		   --and  convert(varchar(4),SPMON,121) = convert(varchar(4),getdate(),121)
		   and  convert(varchar(10),SPMON,121)>=CONVERT(varchar(10),dateadd(month,-1,getdate()),121)
		GROUP BY SPMON,BUKRS)  t1
	GROUP  BY  t1.BUKRS,t1.months
	union   all
	SELECT  '本月'  times_type,t1.months,t1.BUKRS, 
			sum(t1.TOTAL) TOTAL,sum(t1.NWSP) NWSP,sum(t1.NZWS) NZWS,
			sum(t1.CTOTAL) CTOTAL,sum(t1.CNWSP) CNWSP,sum(t1.CNZWS) CNZWS
	FROM
		(select 
			convert(varchar(4),SPMON,121)   years,
			convert(varchar(7),SPMON,121)   months,
			datepart(week,SPMON)            weeks,
			convert(varchar(10),SPMON,121)  days,
			convert(varchar(13),SPMON,121)  times,
			BUKRS,
			SUM(KZWI2) AS TOTAL,
			SUM(CASE WHEN AUART='NWSP' THEN KZWI2 ELSE 0 END ) AS NWSP,
			SUM(CASE WHEN AUART='WSP' THEN KZWI2 ELSE 0 END ) AS NZWS,
			SUM(ORQTY) AS CTOTAL,
			SUM(CASE WHEN AUART='NWSP' THEN ORQTY ELSE 0 END ) AS CNWSP,
			SUM(CASE WHEN AUART='WSP' THEN ORQTY ELSE 0 END ) AS CNZWS
		 from 
		(
				SELECT 
   
						CONVERT(DATETIME,SPMON+'01',23) as SPMON
						,DEMO1 AS  BUKRS
						,AUART
						,SUM(CAST (ORQTY AS INT)) as ORQTY
						,SUM(CAST ( KZWI2 AS float)) as KZWI2
				FROM SAPORDER_SYNC 
				WHERE TYPE=2
				GROUP BY SPMON,DEMO1,AUART
		) as aac
		WHERE  1=1  
		   --and  convert(varchar(4),SPMON,121) = convert(varchar(4),getdate(),121)
		   and  convert(varchar(10),SPMON,121)>=CONVERT(varchar(10),dateadd(month,-0,getdate()),121)
		GROUP BY SPMON,BUKRS)  t1
	GROUP  BY  t1.BUKRS,t1.months
	union   all
	SELECT  '12个月'  times_type,t1.months,t1.BUKRS, 
			sum(t1.TOTAL) TOTAL,sum(t1.NWSP) NWSP,sum(t1.NZWS) NZWS,
			sum(t1.CTOTAL) CTOTAL,sum(t1.CNWSP) CNWSP,sum(t1.CNZWS) CNZWS
	FROM
		(select 
			convert(varchar(4),SPMON,121)   years,
			convert(varchar(7),SPMON,121)   months,
			datepart(week,SPMON)            weeks,
			convert(varchar(10),SPMON,121)  days,
			convert(varchar(13),SPMON,121)  times,
			BUKRS,
			SUM(KZWI2) AS TOTAL,
			SUM(CASE WHEN AUART='NWSP' THEN KZWI2 ELSE 0 END ) AS NWSP,
			SUM(CASE WHEN AUART='WSP' THEN KZWI2 ELSE 0 END ) AS NZWS,
			SUM(ORQTY) AS CTOTAL,
			SUM(CASE WHEN AUART='NWSP' THEN ORQTY ELSE 0 END ) AS CNWSP,
			SUM(CASE WHEN AUART='WSP' THEN ORQTY ELSE 0 END ) AS CNZWS
		 from 
		(
				SELECT 
   
						CONVERT(DATETIME,SPMON+'01',23) as SPMON
						,DEMO1 AS  BUKRS
						,AUART
						,SUM(CAST (ORQTY AS INT)) as ORQTY
						,SUM(CAST ( KZWI2 AS float)) as KZWI2
				FROM SAPORDER_SYNC 
				WHERE TYPE=2
				GROUP BY SPMON,DEMO1,AUART
		) as aac
		WHERE  1=1  
		   --and  convert(varchar(4),SPMON,121) = convert(varchar(4),getdate(),121)
		   and  convert(varchar(10),SPMON,121)>=CONVERT(varchar(10),dateadd(month,-12,getdate()),121)
		GROUP BY SPMON,BUKRS)  t1
	GROUP  BY  t1.BUKRS,t1.months) t2
GROUP  BY  t2.times_type,t2.months,t2.BUKRS;



	/*
	EXEC  P_ETL_SAPDD_DDJCB;
	SELECT  *  FROM  ETL_SAPDD_DDJCB;


	*/