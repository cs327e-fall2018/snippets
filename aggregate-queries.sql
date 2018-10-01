--contributions from various committees to Texas candidates running in 2018 midterm election 
select cm.cand_name, cand_office, cand_pty_affiliation, count(*) as num_contributions, sum(transaction_amt) as contribution_amount 
from fec.candidate_master cm 
join fec.contributions_by_committees cc on cm.cand_id = cc.cand_id
where cand_election_yr = 2018
and cand_office_st = 'TX'
group by cm.cand_name, cand_office, cand_pty_affiliation
having sum(transaction_amt) > 0
order by contribution_amount desc


--organizations who made contributions to various campaign committees for 2018 midterm election
select cc.name, count(cm.cand_id) as num_candidates, sum(cc.transaction_amt) as sum_contributions
from fec.contributions_by_committees cc 
join fec.candidate_master cm on cc.cand_id = cm.cand_id
where cm.cand_election_yr = 2018
and entity_tp = 'ORG'
group by cc.name
order by sum_contributions desc


--contributions by individuals to various campaign committees based in TX
select cm.cmte_nm, cm.cmte_pty_affiliation, count(*) as num_contributions, sum(transaction_amt) as contribution_amount 
from fec.committee_master cm 
join `cs327e-fa2018.fec.contributions_by_individuals_*` ci on cm.cmte_id = ci.cmte_id 
where cmte_st = 'TX'
group by cm.cmte_nm, cm.cmte_pty_affiliation
having sum(transaction_amt) > 0
order by contribution_amount desc