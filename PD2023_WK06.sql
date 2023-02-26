-- pivot wide table into tall table using unpivot function 
-- pivot multiple columns using CTE
-- calculate percentage of total using ratio to report function instead of using subquery
with mobile_app as(
select CUSTOMER_ID, replace(MOBILE_APP_RESPONSES, 'MOBILE_APP___', '') as CATEGORIES, MOBILE_APP_RATING from PD2023_WK06_DSB_CUSTOMER_SURVEY 
unpivot(mobile_app_rating for mobile_app_responses in (MOBILE_APP___EASE_OF_USE, MOBILE_APP___EASE_OF_ACCESS, MOBILE_APP___NAVIGATION, MOBILE_APP___LIKELIHOOD_TO_RECOMMEND, MOBILE_APP___OVERALL_RATING))
),
online_interface as(
(select CUSTOMER_ID as CUSTOMER_ID_2, replace(ONLINE_INTERFACE_RESPONSES, 'ONLINE_INTERFACE___' , '') as CATEGORIES_2, ONLINE_INTERFACE_RATING from PD2023_WK06_DSB_CUSTOMER_SURVEY
unpivot(online_interface_rating for online_interface_responses in (ONLINE_INTERFACE___EASE_OF_USE, ONLINE_INTERFACE___EASE_OF_ACCESS, ONLINE_INTERFACE___NAVIGATION, ONLINE_INTERFACE___LIKELIHOOD_TO_RECOMMEND, ONLINE_INTERFACE___OVERALL_RATING))) 
),
WK06 as(
select mobile_app.CUSTOMER_ID, replace(CATEGORIES, '_', ' ') as CATEGORIES, mobile_app.MOBILE_APP_RATING , online_interface.* exclude(CUSTOMER_ID_2, CATEGORIES_2) from mobile_app inner join online_interface on mobile_app.CUSTOMER_ID = online_interface.CUSTOMER_ID_2 and mobile_app.CATEGORIES = online_interface.CATEGORIES_2 where CATEGORIES <> 'OVERALL_RATING'
),
WK06_PREFERENCE as(
select CUSTOMER_ID, avg(MOBILE_APP_RATING) - avg(ONLINE_INTERFACE_RATING) as DIFF,
CASE 
	WHEN DIFF >= 2 THEN 'Mobile App Superfans'
    WHEN DIFF >= 1 THEN 'Mobile App Fans'
    WHEN DIFF <= -2 THEN 'Online Interface Superfan'
    WHEN DIFF <= -1 THEN 'Online Interface Fans'
    ELSE 'Neutral'
END AS "Preference", 1 as CATEGORIES_COUNT
from WK06 group by CUSTOMER_ID
),
PREFERENCE_GROUP as (
select "Preference", sum(CATEGORIES_COUNT) as CATEGORIES_COUNT from WK06_PREFERENCE group by "Preference"
)
select "Preference", round(100*RATIO_TO_REPORT(CATEGORIES_COUNT) over(order by "Preference"), 1) as "% of Total" from PREFERENCE_GROUP
