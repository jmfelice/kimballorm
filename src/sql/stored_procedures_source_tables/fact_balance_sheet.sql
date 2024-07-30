CREATE OR REPLACE PROCEDURE finance_etl.sp_create_fact_balance_sheet()
AS $$
BEGIN
CALL finance_etl.sp_create_fact_balance_sheet_beginning_balance();

insert into finance_etl.fact_balance_sheet_source
(
    branch_key,
    gl_account_id_key,
    category_key,
    corporation_key,
    posting_date_key,
    debit_balance,
    credit_balance,
    balance
)

WITH all_months as
(
select calendar_date
from finance_dw.dim_calendar
where
    day_of_month = days_in_month and
    calendar_date between
        (select distinct posting_date from ##beginning_balance) and
        (select last_day(max(glperiod)) as dte from finance_staging.iseries_usrjmflib_arfadtv110)
)

, all_combinations as
(
select distinct
cast(branch as INT) as branch,
cast(gl_account_id as VARCHAR(10)) as gl_account_id,
cast(corporation as INT) as corporation,
cast(m.calendar_date as DATE) as posting_date
from (
    select
    rbbr as branch,
    rbglcd as gl_account_id,
    rbcorp as corporation
    from finance_staging.iseries_usrjmflib_arfadtv110
        union all
    select
    branch,
    gl_account_id,
    corporation
    from ##beginning_balance
) f
cross join all_months m
)

, re AS
(
select
f.rbbr      as branch,
f.rbcorp    as corporation ,
date_part(year, last_day(f.glperiod)) + 1 as posting_year,
sum(sum(COALESCE(f.rbdr, 0))) over(partition by f.rbbr, f.rbcorp order by date_part(year, last_day(f.glperiod)) rows unbounded preceding) as credit_amount,
sum(sum(COALESCE(f.rbcr, 0))) over(partition by f.rbbr, f.rbcorp order by date_part(year, last_day(f.glperiod)) rows unbounded preceding) as debit_amount,
sum(sum(COALESCE(f.rbcr, 0) - COALESCE(f.rbdr, 0))) over(partition by f.rbbr, f.rbcorp order by date_part(year, last_day(f.glperiod)) rows unbounded preceding) as amount
from finance_staging.iseries_usrjmflib_arfadtv110 f
left join finance_dw.dim_account a on a.gl_account_id = f.rbglcd
where
    -- date_part(year, last_day(f.glperiod)) + 1 > (select distinct dateadd(year, 1, posting_date) from ##beginning_balance) and
    a.account_class in ('R', 'E')
group by f.rbbr, f.rbcorp, date_part(year, last_day(f.glperiod))
)

, gl AS
(
select
f.rbbr as branch,
f.rbglcd as gl_account_id,
f.rbcorp as corporation ,
last_day(f.glperiod) as posting_date,
sum(COALESCE(f.rbdr, 0)) as debit_amount,
sum(COALESCE(f.rbcr, 0)) as credit_amount,
sum(COALESCE(f.rbdr, 0) - COALESCE(f.rbcr, 0)) as amount
from finance_staging.iseries_usrjmflib_arfadtv110 f
where f.glperiod > (select distinct posting_date from ##beginning_balance) and f.rbdesc != 'Closing Retained'
group by f.rbbr, f.rbglcd, f.rbcorp, f.glperiod
)

, all_data AS
(
select
ac.branch,
ac.gl_account_id,
ac.corporation,
ac.posting_date,
coalesce(bs.beginning_debit_balance , 0) as beginning_debit_balance,
coalesce(bs.beginning_credit_balance, 0) as beginning_credit_balance,
coalesce(bs.beginning_balance       , 0) as beginning_balance,
case
    when a.account_class in ('R', 'E')
    then sum(coalesce(gl.debit_amount, 0)) over(partition by ac.branch, ac.gl_account_id, ac.corporation, date_part(year, ac.posting_date) order by ac.posting_date rows unbounded preceding)
    else sum(coalesce(gl.debit_amount, 0)) over(partition by ac.branch, ac.gl_account_id, ac.corporation order by ac.posting_date rows unbounded preceding)
    end  as ytd_debit_amount,
case
    when a.account_class in ('R', 'E')
    then sum(coalesce(gl.credit_amount, 0)) over(partition by ac.branch, ac.gl_account_id, ac.corporation, date_part(year, ac.posting_date) order by ac.posting_date rows unbounded preceding)
    else sum(coalesce(gl.credit_amount, 0)) over(partition by ac.branch, ac.gl_account_id, ac.corporation order by ac.posting_date rows unbounded preceding)
    end  as ytd_credit_amount,
case
    when a.account_class in ('R', 'E')
    then sum(coalesce(gl.amount, 0)) over(partition by ac.branch, ac.gl_account_id, ac.corporation, date_part(year, ac.posting_date) order by ac.posting_date rows unbounded preceding)
    else sum(coalesce(gl.amount, 0)) over(partition by ac.branch, ac.gl_account_id, ac.corporation order by ac.posting_date rows unbounded preceding)
    end  as ytd_amount,
coalesce(re.debit_amount , 0) as re_debit_amount,
coalesce(re.credit_amount, 0) as re_credit_amount,
coalesce(re.amount       , 0) as re_amount

from all_combinations ac

left join finance_dw.dim_account a on
    a.gl_account_id = ac.gl_account_id

left join ##beginning_balance bs on
    bs.branch        = ac.branch         and
    bs.corporation   = ac.corporation    and
    bs.gl_account_id = ac.gl_account_id

left join gl on
    gl.branch        = ac.branch        and
    gl.corporation   = ac.corporation   and
    gl.gl_account_id = ac.gl_account_id and
    gl.posting_date  = ac.posting_date

left join re on
    re.branch        = ac.branch        and
    re.corporation   = ac.corporation   and
    '31'             = ac.gl_account_id and
    re.posting_year  = date_part(year, ac.posting_date)
)

select
coalesce(b.branch_key, -1) as branch_key,
coalesce(a.gl_account_id_key, -1) as gl_account_id_key,
coalesce(ct.category_key, -1) as category_key,
coalesce(co.corporation_key, -1) as corporation_key,
coalesce(c.date_key, -1) as posting_date_key,
sum(beginning_debit_balance  + ytd_debit_amount  + re_debit_amount ) as debit_balance,
sum(beginning_credit_balance + ytd_credit_amount + re_credit_amount) as credit_balance,
sum(beginning_balance        + ytd_amount        + re_amount       ) as balance
from all_data f
left join finance_dw.dim_account a on a.gl_account_id = f.gl_account_id
left join finance_dw.dim_category ct on ct.category = a.gl_category
left join finance_dw.dim_calendar c on c.calendar_date = f.posting_date
left join finance_dw.dim_branch b on b.branch = f.branch and f.posting_date between b.SCD2_start_date and scd2_end_date
left join finance_dw.dim_corporation co on co.corporation = f.corporation
group by b.branch_key, a.gl_account_id_key, ct.category_key, c.date_key, co.corporation_key

;

END;
$$ LANGUAGE plpgsql;
