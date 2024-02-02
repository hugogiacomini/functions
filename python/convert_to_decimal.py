# Define convert_to_decimal function
def convert_to_decimal(df):
    import pyspark.sql.functions as F
    for column in df.columns:
        # Verifica se a coluna contém apenas números e separadores
        if df.filter(~F.regexp_like(F.col(column), F.lit(r'\.'))).count() == 0:
            df = df.withColumn(column, F.col(column).cast("decimal(10,2)"))
    return df