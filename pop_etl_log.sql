use PH_APDEStore

IF OBJECT_ID('tempdb..#etl') IS NOT NULL DROP TABLE #etl

select a.*, b.tuple AS 'note',
convert(datetime,
	iif(isdate(SUBSTRING(b.tuple, len(b.tuple) - 23, 23)) = 1, 
		SUBSTRING(b.tuple, len(b.tuple) - 23, 23),
		SUBSTRING(b.tuple, len(b.tuple) - 19, 19))) as 'note_dt'
into #etl
from metadata.pop_etl_log a
	cross apply [PH\jwhitehurst].split_str(etl_notes, ';') b

select year, batch_name, geo_type, r_type, cast(max(note_dt) - min(note_dt) as time) as 'process_time'
from #etl
where load_archive_datetime is null and load_ref_datetime is not null
group by year, batch_name, geo_type, r_type
order by year desc, batch_name desc, geo_type, r_type

select top 100 id, batch_name, file_name, note, convert(date, note_dt) as 'note_date', format(note_dt, 'hh:mm tt') as 'note_time'
from #etl
order by note_dt desc

select * from [metadata].[pop_etl_log] where load_ref_datetime is null order by batch_name desc, year, geo_type, r_type, id
select * from [metadata].[pop_etl_log] where load_ref_datetime is not null and load_archive_datetime is null order by year, geo_type, r_type, id
select * from [metadata].[pop_etl_log] where load_archive_datetime is not null order by year, geo_type, r_type, id