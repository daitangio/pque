
select pque_drop_queue('batch_queue');
select pque_drop_queue('delete_queue');
select pque_drop_queue('empty_queue');
select pque_drop_queue('without_delete_queue');

select pque_create('empty_queue');
select pque_create('without_delete_queue');
select pque_create('delete_queue');
select pque_create('batch_queue');

select pque_create('empty_message');
select pque_create('transactional_queue');
select pque_create('wrong_json_message');