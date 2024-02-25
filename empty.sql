# delete from computers where id > -1;

# delete from individual_authorities
# where username IN (select username from individual_login where volunteer_id is not null);
# delete from individual_login
# where volunteer_id is not null;
# delete
# from individuals
# where id > 2;
# truncate ticket_log;
# truncate ticket_status;
# delete
# from tickets
# where id > -1;

# timesheets
truncate timesheets;

commit;
