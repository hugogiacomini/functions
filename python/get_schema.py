def get_schema(df):
    schema = [i.simpleString().replace(':', ' ') for i in df.schema]
    schema = ' ,'.join(map(str, schema))
    return schema