WITH t1 AS(
    SELECT
        * exclude COL_NAME,
        REPLACE(SPLIT(COL_NAME, '_') [0], 'X', '') AS YEAR,
        REPLACE(SPLIT(COL_NAME, '_') [1], '"', '') AS METRIC
    FROM(
            SELECT
                *
            FROM
                TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK21 UNPIVOT(
                    MEASURE FOR COL_NAME IN (
                        "X2021_ATTAINMENT",
                        "X2021_EFFORT",
                        "X2021_ATTENDANCE",
                        "X2021_BEHAVIOUR",
                        "X2022_ATTAINMENT",
                        "X2022_EFFORT",
                        "X2022_ATTENDANCE",
                        "X2022_BEHAVIOUR"
                    )
                )
        )
)
SELECT
    *,
    "2022" - "2021" AS DIFFERENCE, CASE WHEN DIFFERENCE > 0 THEN 'Improvement' WHEN DIFFERENCE < 0 THEN 'Cause for concern' ELSE 'No Change' END AS PROGRESS
FROM(
        SELECT
            STUDENT_ID,
            FIRST_NAME,
            LAST_NAME,
            GENDER,
            D_O_B,
            AVG("2021") AS "2021",
            AVG("2022") AS "2022"
        FROM
            t1 PIVOT(SUM(MEASURE) FOR YEAR IN (2021, 2022))
        GROUP BY
            1,2,3,4,5
    ) WHERE PROGRESS = 'Cause for concern';