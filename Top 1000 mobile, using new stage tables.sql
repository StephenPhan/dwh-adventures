-- Below query is in 3 parts which can all be run seperately for debugging or optimization purposes.

-------------------
--UPDATED AS OF:
--2017-11-14
-------------------

-- Part 1 creates the Top Thousand global table with all pertinent app_ids for either google_play or itunes_connect

select app_id, app_kind
    , BR, CA, CN, DE, FI, FR, GB, JP, KR, MX, RU, SE, US
    ,row_number() over (partition by app_kind order by case when BR is null then 99999 else br end asc) as rank_BR
    ,row_number() over (partition by app_kind order by case when CA is null then 99999 else ca end asc) as rank_CA
    ,row_number() over (partition by app_kind order by case when CN is null then 99999 else cn end asc) as rank_CN
    ,row_number() over (partition by app_kind order by case when DE is null then 99999 else de end asc) as rank_DE
    ,row_number() over (partition by app_kind order by case when FI is null then 99999 else fi end asc) as rank_FI
    ,row_number() over (partition by app_kind order by case when FR is null then 99999 else fr end asc) as rank_FR
    ,row_number() over (partition by app_kind order by case when GB is null then 99999 else gb end asc) as rank_GB
    ,row_number() over (partition by app_kind order by case when JP is null then 99999 else jp end asc) as rank_JP
    ,row_number() over (partition by app_kind order by case when KR is null then 99999 else kr end asc) as rank_KR
    ,row_number() over (partition by app_kind order by case when MX is null then 99999 else mx end asc) as rank_MX
    ,row_number() over (partition by app_kind order by case when RU is null then 99999 else ru end asc) as rank_RU
    ,row_number() over (partition by app_kind order by case when SE is null then 99999 else se end asc) as rank_SE
    ,row_number() over (partition by app_kind order by case when US is null then 99999 else us end asc) as rank_US
into #____midtt____
from (
        select rank_table.app_country_iso, rank_table.app_id, rank_table.app_store_id, app_kind, 
            ((max(days_in_quarter) - count(convert (int, app_rank))) * 541 + sum(convert(int, app_rank))) / max(days_in_quarter) as app_rank --Google Play
            --((max(days_in_quarter) - count(convert (int, app_rank))) * 1501 + sum(convert (int, app_rank))) / max(days_in_quarter) as app_rank --iTunes
        from dw_stage.apptopia.app_ranks as rank_table
        join (
                select app_country_iso, count(distinct app_date) as days_in_quarter
                from dw_stage.apptopia.app_ranks
                where app_date >= '2017-07-01' and app_date < '2017-10-01' --dates delimiting a quarter
                    and [app_store_id] in ('google_play')
                    --and [app_store_id] in ('itunes_connect')
                    and app_category_id in ('38','6014')  
                    and app_kind in ('grossing', 'free') 
                    group by app_country_iso
        ) day_count on rank_table.app_country_iso = day_count.app_country_iso
        where rank_table.app_store_id in ('google_play')  
            --rank_table.app_store_id in ('itunes_connect')
            and app_category_id in ('38', '6014')
            and app_date >= '2017-07-01' and app_date < '2017-10-01'
            and app_kind in ('grossing', 'free')
            and rank_table.app_country_iso in ('BR','CA','CN','DE','FI','FR','GB','JP','KR','MX','RU','SE','US')
        group by rank_table.app_country_iso, rank_table.app_id, app_store_id, app_kind
    ) as sourcetable
PIVOT(
    avg(app_rank) for app_country_iso in (BR, CA, CN, DE, FI, FR, GB, JP, KR, MX, RU, SE, US)
) as pivottable

select * 
into ##____top1k____
from #____midtt____
where (rank_BR<1001
    OR rank_CA<1001
    OR rank_CN<1001
    OR rank_DE<1001
    OR rank_FI<1001
    OR rank_FR<1001
    OR rank_GB<1001
    OR rank_JP<1001
    OR rank_KR<1001
    OR rank_MX<1001
    OR rank_RU<1001
    OR rank_SE<1001
    OR rank_US<1001)

---------------------------------------------------------------------------------------------------------

-- Part 2 amends all info related to the app including SDK analysis

select app_id, app_name, app_publisher_name, app_price_cents
    ,case when unity is null then 0 else 1 end as unity
    ,case when [Unreal Engine] is null then 0 else 1 end as unreal
    ,case when cocos2d is null then 0 else 1 end cocos
    ,case when marmalade is null then 0 else 1 end as marmalade
    ,case when corona is null then 0 else 1 end as corona
    ,case when [Corona Labs] is null then 0 else 1 end as corona_labs
    ,case when Xamarin is null then 0 else 1 end as xamarin
into ##____ainfo____
from 
    (
        select md.app_id, app_name, app_publisher_name, app_sdk_name,'1' as sdk_present, app_price_cents from dw_stage.apptopia.app_metadata md
        right join ##____top1k____ tt on md.app_id = tt.app_id
        where app_store_id = 'google_play' --to avoid using CN Android stores
    ) as sourcetable2
PIVOT(
    max(sdk_present) for app_sdk_name in (Unity, [Unreal Engine], Cocos2D, Marmalade, Corona, [Corona Labs], Xamarin)
) as pivottable2

---------------------------------------------------------------------------------------------------------

-- Part 3 joins both tables for final output

select distinct a.app_id, a.app_kind, app_name, app_publisher_name, app_price_cents--, initial_release_date
    ,BR, CA, CN, DE, FI, FR, GB, JP, KR, MX, RU, SE, US
    ,rank_BR, rank_CA, rank_CN, rank_DE, rank_FI, rank_FR, rank_GB, rank_JP, rank_KR, rank_MX, rank_RU, rank_SE, rank_US
    ,case when b.app_id is not NULL then 'found' else 'missing' end as missingSDK
    ,unity, unreal, cocos, marmalade, corona, corona_labs, xamarin
from ##____top1k____ a
left join ##____ainfo____ b on a.app_id = b.app_id
