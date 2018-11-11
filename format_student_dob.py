import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn performs processing on each element from the input PCollection.
class FormatDobFn(beam.DoFn):
  def process(self, element):
    record = element
    input_dob = record.get('dob')
    
    # desired date format: YYYY-MM-DD (e.g. 2000-09-30)
    # input date formats: MM/DD/YYYY or YYYY-MM-DD
    dob_split = input_dob.split('/')
    if len(dob_split) > 1:
        month = dob_split[0]
        day = dob_split[1]
        year = dob_split[2]
        reformatted_dob = year + '-' + month + '-' + day
        record['dob'] = reformatted_dob
    return [record]

# Project is needed for bigquery data source, even with local execution.
options = {
    'project': 'cs327e-fa2018'
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DirectRunner', options=opts) as p:
    
    query_results = p | beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM college_split1.Student'))

    # write PCollection to a log file
    query_results | 'Write to File 1' >> WriteToText('query_results.txt')

    # apply a ParDo to the PCollection 
    out_pcoll = query_results | 'Format DOB' >> beam.ParDo(FormatDobFn())
    
    # write PCollection to a log file
    out_pcoll | 'Write to File 2' >> WriteToText('output_pardo.txt')
    
    qualified_table_name = 'cs327e-fa2018:college_split2.Formatted_Student'
    table_schema = 'sid:STRING,fname:STRING,lname:STRING,dob:DATE'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                     schema=table_schema,  
                                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)