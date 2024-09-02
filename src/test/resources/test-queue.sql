
select pigi_drop_queue('batch_queue');
select pigi_drop_queue('delete_queue');
select pigi_drop_queue('empty_queue');
select pigi_drop_queue('without_delete_queue');

select pigi_create('empty_queue');
select pigi_create('without_delete_queue');
select pigi_create('delete_queue');
select pigi_create('batch_queue');

select pigi_create('empty_message');
select pigi_create('transactional_queue');
select pigi_create('wrong_json_message');