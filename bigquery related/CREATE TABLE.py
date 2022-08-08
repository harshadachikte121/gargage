import os
from google.cloud import bigquery

if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\SPARK\service_key_google_cloud.json"
    obj_client = bigquery.Client()

    schema = [
        bigquery.SchemaField(name="stud_id",field_type= "INTEGER"),
        bigquery.SchemaField(name="stud_name",field_type="STRING"),
        bigquery.SchemaField(name="gender",field_type="string"),
        bigquery.SchemaField(name="admission_date",field_type="DATE")
     ]
    table = bigquery.Table("semiotic-anvil-351807.bwt_session_dataset.student_td", schema=schema)
    print("table object created: {}".format(table))
    table.time_partitioning = bigquery.TimePartitioning(
        field= "admission_date"
    )
    table = obj_client.create_table(table)
    print("successfully created table: {}".format(table.table_id))