import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# Project ID is needed for bigquery data source, even with local execution.
options = {
    'project': 'cs327e-fa2018'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    students_pcoll = p | 'Read Student' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM college_split2.Formatted_Student'))

    # write PCollection to a log file
    students_pcoll | 'Write to File 1' >> WriteToText('student_query_results.txt')
    
    new_students_pcoll = p | 'Read New Student' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM college_split1.New_Student'))

    # write PCollection to a log file
    new_students_pcoll | 'Write to File 2' >> WriteToText('new_student_query_results.txt')

    # Flatten the two PCollections 
    merged_pcoll = (students_pcoll, new_students_pcoll) | 'Merge Students and New Students' >> beam.Flatten()
    
    # write PCollection to a file
    merged_pcoll | 'Write to File 3' >> WriteToText('output_flatten.txt')
    
    qualified_table_name = 'cs327e-fa2018:college_split2.Merged_Student'
    table_schema = 'sid:STRING,fname:STRING,lname:STRING,dob:DATE'
    
    merged_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)