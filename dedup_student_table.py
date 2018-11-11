import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class MakeStudentTuple(beam.DoFn):
  def process(self, element):
    record = element
    student_tuple = (record, '')
    return [student_tuple]

class MakeStudentRecord(beam.DoFn):
  def process(self, element):
    record, val = element
    return [record]

# Project ID is needed for bigquery data source, even with local execution.
options = {
    'project': 'cs327e-fa2018'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM college_split2.Merged_Student'))

    # write PCollection to a log file
    query_results | 'Write to File 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    tuple_pcoll = query_results | 'Create Student Tuple' >> beam.ParDo(MakeStudentTuple())
    
    # write PCollection to a log file
    tuple_pcoll | 'Write to File 2' >> WriteToText('output_pardo_student_tuple.txt')
    
    deduped_pcoll = tuple_pcoll | 'Dedup Student Records' >> beam.GroupByKey()
    
    # write PCollection to a log file
    deduped_pcoll | 'Write to File 3' >> WriteToText('output_group_by_key.txt')
    
    # apply a second ParDo to the PCollection 
    out_pcoll = deduped_pcoll | 'Create Student Record' >> beam.ParDo(MakeStudentRecord())
    
    # write PCollection to a log file
    out_pcoll | 'Write to File 4' >> WriteToText('output_pardo_student_record.txt')
    
    qualified_table_name = 'cs327e-fa2018:college_split2.Deduped_Student'
    table_schema = 'sid:STRING,fname:STRING,lname:STRING,dob:DATE'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)