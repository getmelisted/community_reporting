Select `Client Name`, `Client ID`, `Location Branch ID`, `Account Manager`, `Location Name`, `Location Address`, l2.wo_id as 'Failing Work Order',
l2.log_desc as "Last API Error Message", 
d.dir_name as 'Directory Name', l2.log_id as 'Last Log ID', l2.loc_id as 'Location ID', l2.dir_id as 'Directory ID', l2.log_date as 'Date of last API Error'
from (
	Select MAX(log.log_id) as max_log, l.loc_id, c.client_name as 'Client Name', c.client_id as 'Client ID', l.loc_branchid as 'Location Branch ID',
     l.loc_name as 'Location Name', u.user_name as 'Account Manager',
	l.loc_addr1 as 'Location Address' 
	from wo 
	inner join client c on c.client_id = wo.client_id and c.client_status = 2 
	inner join location l on l.loc_id = wo.loc_id
	inner join log on log.wo_id = wo.wo_id and (log.log_type = 4 OR (log.log_type = 0 and log.user_id > 1 and log.log_desc not like '%[BULK]%' ))
	inner join user u on c.client_am = u.user_id
    inner join (Select MAX(wo_id) max_wo from wo where wo.wo_expireddate is null and wo.dir_id in (1,29,37,48,88,158,176) group by loc_id, dir_id) wo2 on wo2.max_wo = wo.wo_id
    
	where wo.wo_status = 2 and wo.wo_expireddate is null 
		and (l.loc_canceldate is null OR l.loc_canceldate > current_date()) and (c.client_canceldate is null OR c.client_canceldate > current_date())
        and (l.loc_closeddoorsdate is null or l.loc_closeddoorsdate > current_date())
		and wo.dir_id in (1,29,37,48,88,158,176)
	group by l.loc_id, wo.wo_id
    ) as cte
inner join log l2 on l2.log_id = cte.max_log
inner join directory d on d.dir_id = l2.dir_id