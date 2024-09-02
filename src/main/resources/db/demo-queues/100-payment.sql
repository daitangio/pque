-- liquibase formatted sql
-- changeset GG:1 runOnChange:true
-- comment: Very simple payment queues

select pigi_create('market_request');
select pigi_create('market_response');
