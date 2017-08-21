--QUARTERLY MOBILE MARKET SHARE
--The query is split into three tables: TOPTHOUSAND, APP_SDK & INFORMATION
--This code is double indented for readability

WITH TOPTHOUSAND as (
        --The following table creates a pivot which outputs the estimated ranking of an app_id in a given country's rank list.
        --By creating an averaged rank value and having SQL Server apply a hierarchical numeration, we can label any sized 'top' list. 
        --To calculate GP or iOS we must comment in/out two WHERE clauses on lines 26/27 and 35.
        --Google Play creates daily rank lists of 540. iOS App Store has a list of 1500.
        SELECT app_id, app_kind
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
        from (
                SELECT rank_table.app_country, rank_table.app_id, rank_table.app_store_id, app_kind, 
                ((max(days_in_quarter) - count(app_rank)) * 541 + sum(app_rank)) / max(days_in_quarter) as app_rank --Google Play
                --((max(days_in_quarter) - count(app_rank)) * 1501 + sum(app_rank)) / max(days_in_quarter) as app_rank --iTunes
                from dw_stage.apptopia.rank_lists as rank_table
                join (
                        SELECT app_country, count(distinct app_date) as days_in_quarter
                        from dw_stage.apptopia.rank_lists
                        where app_date >= '2017-04-01' and app_date < '2017-07-01' --dates delimiting a quarter
			AND [app_store_id] in ('google_play'/*'itunes_connect'*/)
			AND app_category_id in ('38','6014')  
			AND app_kind in ('grossing', 'free') 
                        group by app_country
            ) day_count on rank_table.app_country = day_count.app_country
            where rank_table.app_store_id in ('google_play'/*'itunes_connect'*/)
            and app_category_id in ('38', '6014')
            and app_date >= '2017-04-01' and app_date < '2017-07-01'
            and app_kind in ('grossing', 'free')
            and rank_table.app_country in ('BR','CA','CN','DE','FI','FR','GB','JP','KR','MX','RU','SE','US')
            group by rank_table.app_country, rank_table.app_id, app_store_id, app_kind
        ) as sourcetable
        PIVOT(
                avg(app_rank) for app_country in (BR, CA, CN, DE, FI, FR, GB, JP, KR, MX, RU, SE, US)
        ) as pivottable

)
,

APP_SDK as (
        --The following table is used to ID engine SDKs for all ranked apps and those without any SDK recognition.
        SELECT distinct app_name, app_id, app_store_id
        ,case when sdk_id is not null then 'found' else 'missing' end as missingSDK
        ,case when unity is null then 0 else 1 end as unity
        ,case when [Unreal Engine] is null then 0 else 1 end as unreal
        ,case when cocos2d is null then 0 else 1 end coco
        ,case when marmalade is null then 0 else 1 end as marmalade
        ,case when corona is null then 0 else 1 end as corona
        ,case when [Corona Labs] is null then 0 else 1 end as corona_labs
        ,case when Xamarin is null then 0 else 1 end as xamarin
        from (
                SELECT app_table.app_name, app_table.app_id, app_table.app_store_id, sdk_table.sdk_id, sdk_name, sdk_present
                from dw_stage.apptopia.app as app_table
                left join dw_stage.apptopia.app_sdks as sdk_table on app_table.app_id = sdk_table.app_id
				join dw_stage.apptopia.sdk as sdk_string on sdk_table.sdk_id = sdk_string.sdk_id
                where category_id in ('38', '6014')
                and app_table.app_store_id in ('google_play', 'itunes_connect')
                and sdk_present = 1
        ) as sourcetable2
        PIVOT(
                max(sdk_present) for sdk_name in (Unity, [Unreal Engine], Cocos2D, Marmalade, Corona, [Corona Labs], Xamarin)
        ) as pivottable2
)
--,

--INFORMATION as (
--        --The following table is used to determine the price of an app and its publisher.
--        SELECT distinct app_id, app_store_id, price, publisher_name
--        ,genreAction = (case when subcategory_id in (90, 7001) then 1 else 0 end)
--        ,genreAdventure = (case when subcategory_id in (89, 7002) then 1 else 0 end)
--        ,genreArcade = (case when subcategory_id in (84, 7003) then 1 else 0 end) 
--        ,genreBoard = (case when subcategory_id in (92, 7004) then 1 else 0 end) 
--        ,genreCard = (case when subcategory_id in (83, 7005) then 1 else 0 end) 
--        ,genreCasino = (case when subcategory_id in (86, 7006) then 1 else 0 end) 
--        ,genreCasual = (case when subcategory_id in (4) then 1 else 0 end) 
--        ,genreDice = (case when subcategory_id in (7007) then 1 else 0 end) 
--        ,genreEducational = (case when subcategory_id in (95, 7008) then 1 else 0 end) 
--        ,genreFamily = (case when subcategory_id in (88, 7009) then 1 else 0 end) 
--        ,genreLiveWallpaper = (case when subcategory_id in (40) then 1 else 0 end) 
--        ,genreMusic = (case when subcategory_id in (94, 7011) then 1 else 0 end) 
--        ,genrePuzzle = (case when subcategory_id in (85, 7012) then 1 else 0 end) 
--        ,genreRacing = (case when subcategory_id in (6, 7013) then 1 else 0 end) 
--        ,genreRolePlaying = (case when subcategory_id in (97, 7014) then 1 else 0 end) 
--        ,genreSimulation = (case when subcategory_id in (93, 7015) then 1 else 0 end) 
--        ,genreSports = (case when subcategory_id in (7, 7016) then 1 else 0 end) 
--        ,genreStrategy = (case when subcategory_id in (96, 7017) then 1 else 0 end) 
--        ,genreTrivia = (case when subcategory_id in (91, 7018) then 1 else 0 end) 
--        ,genreWidgets = (case when subcategory_id in (41) then 1 else 0 end) 
--        ,genreWord = (case when subcategory_id in (87, 7019) then 1 else 0 end) 
--        from(
--                SELECT distinct a.app_id, a.category_id, a.app_store_id, a.publisher_id, p.publisher_name, a.subcategory_id
--                , row_number() over(partition by app_id order by a.dwloaddate desc) as rn
--                , case when app_price_cents = 0 then 'free' else 'paid' end as price
--                from dw_stage.apptopia.app as a
--                left join dw_stage.apptopia.publisher as p on a.publisher_id = p.publisher_id
--        ) as pub
--        where rn = 1
--)

SELECT distinct
TOPTHOUSAND.app_id
--,INFORMATION.publisher_name
--,INFORMATION.app_store_id
,TOPTHOUSAND.app_kind as [ranking category]
--,INFORMATION.price
--,INFORMATION.genreAction
--,INFORMATION.genreAdventure
--,INFORMATION.genreArcade
--,INFORMATION.genreBoard
--,INFORMATION.genreCard
--,INFORMATION.genreCasino
--,INFORMATION.genreCasual
--,INFORMATION.genreDice
--,INFORMATION.genreEducational
--,INFORMATION.genreFamily
--,INFORMATION.genreLiveWallpaper
--,INFORMATION.genreMusic
--,INFORMATION.genrePuzzle
--,INFORMATION.genreRacing
--,INFORMATION.genreRolePlaying
--,INFORMATION.genreSimulation
--,INFORMATION.genreSports
--,INFORMATION.genreStrategy
--,INFORMATION.genreTrivia
--,INFORMATION.genreWidgets
--,INFORMATION.genreWord
, BR, CA, CN, DE, FI, FR, GB, JP, KR, MX, RU, SE, US
,TOPTHOUSAND.rank_BR
,TOPTHOUSAND.rank_CA
,TOPTHOUSAND.rank_CN
,TOPTHOUSAND.rank_DE
,TOPTHOUSAND.rank_FI
,TOPTHOUSAND.rank_FR
,TOPTHOUSAND.rank_GB
,TOPTHOUSAND.rank_JP
,TOPTHOUSAND.rank_KR
,TOPTHOUSAND.rank_MX
,TOPTHOUSAND.rank_RU
,TOPTHOUSAND.rank_SE
,TOPTHOUSAND.rank_US
,APP_SDK.*
from TOPTHOUSAND
left join APP_SDK on TOPTHOUSAND.app_id = APP_SDK.app_id
--left join INFORMATION on TOPTHOUSAND.app_id = INFORMATION.app_id
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
