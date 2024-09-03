-- liquibase formatted sql
-- changeset GG:1 runOnChange:true
-- comment: Very simple payment queues

select pque_drop_queue('market_request');
select pque_drop_queue('market_response');
-- Super fast unlogged: faster but not replicated and not crash safe
select pque_create_unlogged('market_request');
-- select pque_create('market_request');
select pque_create('market_response');
