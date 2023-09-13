WITH WK20_RESULT AS(
    SELECT
        MI.*,
        MP.* exclude MEAL_OPTION
    FROM(
            SELECT
                * exclude (TYPE, X),
                CASE
                    WHEN TYPE = 'Meat based' THEN 'Meat-based'
                    WHEN TYPE = 'Veggie' THEN 'Vegetarian'
                    ELSE TYPE
                END AS MEAL_TYPE
            FROM
                TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK20_MEAL_NUTRITIONAL_INFO
        ) AS MI
        JOIN TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK20_MEAL_PRICES AS MP ON MI.MEAL_OPTION = MP.MEAL_OPTION
)
SELECT
    MEAL_TYPE,
    ROUND(AVG(PRICE), 2) AS AVG_PRICE,
    ROUND(100 * RATIO_TO_REPORT(COUNT(*)) OVER(), 2) AS PERCENT_TOTAL
FROM
    WK20_RESULT
GROUP BY
    MEAL_TYPE;