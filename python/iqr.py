# Create outlier function to calculate iterquartile range (IQR)
def iqr_outlier(df, col, col_name):
    """
    Calculates the interquartile range (IQR) of a DataFrame column and classifies each
    record as an "up_outlier" or "down_outlier" based on whether they fall outside the
    range or not.

    :param df: The dataframe containing the column
    :type df: :class:`pyspark.sql.DataFrame`

    :param col: The name of the column to perform the outlier detection on.
    :type col: str

    :param up_down: If "up", classifies as an "up_outlier" if outside the IQR range,
                    if "down", classifies as a "down_outlier" if outside the IQR range
    :type up_down: str

    :return: A DataFrame with an additional column "Outlier Type" containing either "up_outlier",
            "down_outlier", or "Not an outlier" depending on the result of the outlier detection.
    :rtype: :class:`pyspark.sql.DataFrame`
    """
    df = df.withColumn(col, F.col(col).cast('double'))
    Q1 = df.approxQuantile(col, [0.25], 0)[0]
    Q3 = df.approxQuantile(col, [0.75], 0)[0]
    IQR = Q3 - Q1
    df = df.withColumn(
        col_name, 
        F.when((F.col(col) > (Q3 + 2 * IQR)), 'up_outlier').otherwise(
            F.when((F.col(col) < (Q1 - 2 * IQR)), 'down_outlier').otherwise('Not an outlier')
        )
    )
    return df